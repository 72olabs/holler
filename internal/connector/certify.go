package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/72olabs/holler/internal/bus"
)

type EventReader interface {
	ListEvents(context.Context, string, string, int64, int) ([]bus.Event, error)
}

type CertificationConfig struct {
	Harness          string
	Profile          string
	ProjectID        string
	Actor            string
	RunID            string
	AfterDurable     int64
	AfterOperational int64
	AttentionMode    string
}

type CertificationReport struct {
	SchemaVersion       int               `json:"schema_version"`
	Harness             string            `json:"harness"`
	Profile             string            `json:"profile"`
	ProjectID           string            `json:"project_id"`
	Actor               string            `json:"actor"`
	RunID               string            `json:"run_id"`
	State               ReadinessState    `json:"state"`
	Ready               bool              `json:"ready"`
	AfterDurable        int64             `json:"after_durable"`
	AfterOperational    int64             `json:"after_operational"`
	ObservedDurable     int64             `json:"observed_durable"`
	ObservedOperational int64             `json:"observed_operational"`
	ObservedBuilds      []string          `json:"observed_builds,omitempty"`
	AttentionMode       string            `json:"attention_mode,omitempty"`
	Checks              []DiagnosticCheck `json:"checks"`
}

func Certify(ctx context.Context, reader EventReader, config CertificationConfig) (CertificationReport, error) {
	manifest, err := Manifest(config.Harness)
	if err != nil {
		return CertificationReport{}, err
	}
	if strings.TrimSpace(config.Profile) == "" {
		config.Profile = "async-peer"
	}
	profile, ok := manifest.Profile(config.Profile)
	if !ok {
		return CertificationReport{}, fmt.Errorf("unsupported connector profile %q", config.Profile)
	}
	if strings.TrimSpace(config.ProjectID) == "" || strings.TrimSpace(config.Actor) == "" || strings.TrimSpace(config.RunID) == "" {
		return CertificationReport{}, &bus.ValidationError{Field: "certification", Problem: "project, actor, and run are required"}
	}
	expectedAdapter := ""
	if profile.RequiresWake {
		expectedAdapter, err = certificationAttentionMode(manifest.Harness, config.AttentionMode)
		if err != nil {
			return CertificationReport{}, err
		}
	}
	durable, durablePosition, err := readCertificationEvents(ctx, reader, config.ProjectID, "durable", config.AfterDurable)
	if err != nil {
		return CertificationReport{}, err
	}
	operational, operationalPosition, err := readCertificationEvents(ctx, reader, config.ProjectID, "operational", config.AfterOperational)
	if err != nil {
		return CertificationReport{}, err
	}
	report := CertificationReport{
		SchemaVersion: 1, Harness: manifest.Harness, Profile: profile.Name, ProjectID: config.ProjectID,
		Actor: config.Actor, RunID: config.RunID, State: StateConfigured,
		AfterDurable: config.AfterDurable, AfterOperational: config.AfterOperational,
		ObservedDurable: durablePosition, ObservedOperational: operationalPosition,
		AttentionMode: expectedAdapter,
	}

	sessions := correlatedSessions(operational, config.Actor, config.RunID, manifest.Harness, expectedAdapter)
	registered := len(sessions) > 0
	hydrated := registered
	wrote := hasMCPRunEvent(durable, "message.sent", config.Actor, config.RunID)
	claimed := mcpMessageIDs(operational, "delivery.claimed", config.Actor, config.RunID)
	acked := mcpMessageIDs(operational, "delivery.acked", config.Actor, config.RunID)
	readAndAcked := intersects(claimed, acked)
	processed := intersection(claimed, acked)
	notified := intersects(processed, acceptedNotificationMessageIDs(
		operational, config.Actor, config.RunID, manifest.Harness, expectedAdapter, sessions,
	))
	var clientBuildSeen, daemonBuildSeen bool
	report.ObservedBuilds, clientBuildSeen, daemonBuildSeen = observedBuildsForRun(
		append(append([]bus.Event{}, durable...), operational...), config.Actor, config.RunID,
	)
	buildsOK := certifiedBuilds(report.ObservedBuilds, clientBuildSeen, daemonBuildSeen)

	report.addEvidence("canary.registration", registered, "real harness session registered with the expected actor and run")
	report.addEvidence("canary.hydration", hydrated, "SessionStart hydration executed for the expected run")
	report.addEvidence("canary.write", wrote, "real client sent through the frozen MCP surface")
	report.addEvidence("canary.claim_ack", readAndAcked, "real client claimed and acknowledged the same durable message")
	buildSummary := "certificate evidence names clean client and daemon builds"
	if !clientBuildSeen {
		buildSummary = "certificate evidence is missing the self-reported client build identity"
	} else if !daemonBuildSeen {
		buildSummary = "certificate evidence is missing the daemon-attested build identity"
	} else if !buildsOK {
		buildSummary = "certificate evidence contains an unknown or dirty build identity"
	}
	report.addEvidence("canary.build_identity", buildsOK, buildSummary)
	report.Checks[len(report.Checks)-1].Evidence = map[string]string{
		"client_build_seen": strconv.FormatBool(clientBuildSeen),
		"daemon_build_seen": strconv.FormatBool(daemonBuildSeen),
		"observed_builds":   strings.Join(report.ObservedBuilds, ", "),
	}
	if !buildsOK {
		report.Checks[len(report.Checks)-1].Remediation = "build client and daemon from a clean commit with scripts/build.sh, restart hollerd, then rerun the canary; inspect observed_builds in this report"
	}
	if profile.RequiresWake {
		summary := "live notification path accepted the same message the client claimed and acknowledged"
		if expectedAdapter != "" {
			summary += " through " + expectedAdapter
		}
		report.addEvidence("canary.notification", notified, summary)
	}

	coreReady := registered && hydrated && wrote && readAndAcked && buildsOK
	switch {
	case coreReady && (!profile.RequiresWake || notified):
		report.State, report.Ready = StateReady, true
	case registered && coreReady && profile.RequiresWake:
		report.State = StateDegraded
	case registered:
		report.State = StateRegistered
	default:
		report.State = StateConfigured
	}
	return report, nil
}

func certificationAttentionMode(harness, configured string) (string, error) {
	mode := strings.TrimSpace(configured)
	switch harness {
	case "codex":
		if mode == "" {
			mode = AttentionNativeQueue
		}
		if err := ValidateCodexAttentionMode(mode); err != nil {
			return "", err
		}
	case "claude":
		if mode == "" {
			mode = AttentionHookLongPoll
		}
		if err := ValidateClaudeAttentionMode(mode); err != nil {
			return "", err
		}
	case "opencode":
		if mode == "" {
			mode = AttentionNativePrompt
		}
		if err := ValidateOpenCodeAttentionMode(mode); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unsupported harness %q", harness)
	}
	if mode == AttentionStartupOnly {
		return "", fmt.Errorf("startup-only cannot satisfy live-review certification")
	}
	return mode, nil
}

func certifiedBuilds(builds []string, clientBuildSeen, daemonBuildSeen bool) bool {
	if !clientBuildSeen || !daemonBuildSeen || len(builds) == 0 {
		return false
	}
	for _, build := range builds {
		if strings.Contains(build, "@unknown") || strings.HasSuffix(build, "+dirty") {
			return false
		}
	}
	return true
}

func readCertificationEvents(ctx context.Context, reader EventReader, project, stream string, after int64) ([]bus.Event, int64, error) {
	position := after
	all := make([]bus.Event, 0)
	for {
		if len(all) >= 10000 {
			extra, err := reader.ListEvents(ctx, project, stream, position, 1)
			if err != nil {
				return nil, position, err
			}
			if len(extra) == 0 {
				return all, position, nil
			}
			return nil, position, fmt.Errorf("certification event window exceeded 10000 events; use newer --after cursors")
		}
		events, err := reader.ListEvents(ctx, project, stream, position, 100)
		if err != nil {
			return nil, position, err
		}
		if len(events) == 0 {
			return all, position, nil
		}
		all = append(all, events...)
		position = events[len(events)-1].Position
		if len(events) < 100 {
			return all, position, nil
		}
	}
}

func hasRunEvent(events []bus.Event, kind, actor, runID, harness string) bool {
	for _, event := range events {
		if event.Kind != kind || event.ActorID != actor {
			continue
		}
		payload := eventPayload(event)
		observedRun := stringValue(payload, "run_id", "from_run")
		if observedRun != runID {
			continue
		}
		if harness == "" || stringValue(payload, "harness") == harness {
			return true
		}
	}
	return false
}

func acceptedNotificationMessageIDs(events []bus.Event, actor, runID, harness, adapter string, sessions map[string]struct{}) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, event := range events {
		if event.Kind != "delivery.notification" || event.ActorID != actor {
			continue
		}
		payload := eventPayload(event)
		if stringValue(payload, "run_id") != runID || stringValue(payload, "harness") != harness {
			continue
		}
		if _, ok := sessions[stringValue(payload, "session_id")]; !ok {
			continue
		}
		if adapter != "" && stringValue(payload, "detail") != adapter {
			continue
		}
		if stringValue(payload, "result") == "accepted" && stringValue(payload, "client") == "hollerd/0.1" && eventBuildsClean(payload) {
			ids[event.MessageID] = struct{}{}
		}
	}
	return ids
}

func correlatedSessions(events []bus.Event, actor, runID, harness, adapter string) map[string]struct{} {
	registered := make(map[string]struct{})
	hydrated := make(map[string]struct{})
	for _, event := range events {
		if event.ActorID != actor {
			continue
		}
		payload := eventPayload(event)
		if stringValue(payload, "run_id", "from_run") != runID || stringValue(payload, "harness") != harness || !eventBuildsClean(payload) {
			continue
		}
		sessionID := stringValue(payload, "session_id")
		if sessionID == "" {
			continue
		}
		switch event.Kind {
		case "session.registered":
			if adapter != "" && stringValue(payload, "attention_mode") != adapter {
				continue
			}
			registered[sessionID] = struct{}{}
		case "startup.hydrated":
			hydrated[sessionID] = struct{}{}
		}
	}
	for sessionID := range registered {
		if _, ok := hydrated[sessionID]; !ok {
			delete(registered, sessionID)
		}
	}
	return registered
}

func hasMCPRunEvent(events []bus.Event, kind, actor, runID string) bool {
	return len(mcpMessageIDs(events, kind, actor, runID)) > 0
}

func mcpMessageIDs(events []bus.Event, kind, actor, runID string) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, event := range events {
		if event.Kind != kind || event.ActorID != actor || event.MessageID == "" {
			continue
		}
		payload := eventPayload(event)
		if stringValue(payload, "run_id", "from_run") == runID && strings.HasPrefix(stringValue(payload, "client"), "bus-mcp/") && eventBuildsClean(payload) {
			ids[event.MessageID] = struct{}{}
		}
	}
	return ids
}

func observedBuildsForRun(events []bus.Event, actor, runID string) ([]string, bool, bool) {
	seen := make(map[string]struct{})
	clientSeen := false
	daemonSeen := false
	for _, event := range events {
		if event.ActorID != actor {
			continue
		}
		payload := eventPayload(event)
		if stringValue(payload, "run_id", "from_run") != runID {
			continue
		}
		if value := stringValue(payload, "client_build"); value != "" {
			clientSeen = true
			seen[value] = struct{}{}
		}
		if value := stringValue(payload, "daemon_build"); value != "" {
			daemonSeen = true
			seen[value] = struct{}{}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values, clientSeen, daemonSeen
}

func eventBuildsClean(payload map[string]interface{}) bool {
	clientBuild := stringValue(payload, "client_build")
	daemonBuild := stringValue(payload, "daemon_build")
	return certifiedBuilds([]string{clientBuild, daemonBuild}, clientBuild != "", daemonBuild != "")
}

func messageIDs(events []bus.Event, kind, actor string) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, event := range events {
		if event.Kind == kind && event.ActorID == actor && event.MessageID != "" {
			ids[event.MessageID] = struct{}{}
		}
	}
	return ids
}

func intersects(left, right map[string]struct{}) bool {
	for value := range left {
		if _, ok := right[value]; ok {
			return true
		}
	}
	return false
}

func intersection(left, right map[string]struct{}) map[string]struct{} {
	values := make(map[string]struct{})
	for value := range left {
		if _, ok := right[value]; ok {
			values[value] = struct{}{}
		}
	}
	return values
}

func eventPayload(event bus.Event) map[string]interface{} {
	var payload map[string]interface{}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload
}

func stringValue(payload map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := payload[key].(string); ok {
			return value
		}
	}
	return ""
}

func (report *CertificationReport) addEvidence(id string, passed bool, summary string) {
	status := CheckPass
	remediation := ""
	if !passed {
		status = CheckFail
		remediation = "rerun one bounded real-client canary after the prior event cursors"
	}
	report.Checks = append(report.Checks, DiagnosticCheck{
		ID: id, Layer: "real-client", Status: status, Summary: summary, Remediation: remediation,
	})
}
