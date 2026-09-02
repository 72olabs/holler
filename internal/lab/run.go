package lab

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/bus"
)

type Config struct {
	Scenario        Scenario
	HollerdBinary   string
	OutputDir       string
	KeepSandbox     bool
	IncludeDatabase bool
	Timeout         time.Duration
}

type AssertionResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

type StepResult struct {
	Index  int       `json:"index"`
	Action string    `json:"action"`
	At     time.Time `json:"at"`
	Passed bool      `json:"passed"`
	Detail string    `json:"detail,omitempty"`
}

type ResourceUsage struct {
	ModelTokens int64 `json:"model_tokens"`
	Processes   int   `json:"processes"`
	Messages    int   `json:"messages"`
}

type ParticipantResult struct {
	ID             string `json:"id"`
	Harness        string `json:"harness"`
	RequestedActor string `json:"requested_actor"`
	AssignedActor  string `json:"assigned_actor"`
	RunID          string `json:"run_id"`
	Generations    int    `json:"generations"`
}

type EventEvidence struct {
	ID          string    `json:"event_id"`
	PartitionID string    `json:"partition_id"`
	Stream      string    `json:"stream"`
	Position    int64     `json:"position"`
	Kind        string    `json:"kind"`
	MessageID   string    `json:"message_id,omitempty"`
	ActorID     string    `json:"actor_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type Report struct {
	SchemaVersion int                 `json:"schema_version"`
	Scenario      string              `json:"scenario"`
	Description   string              `json:"description,omitempty"`
	Passed        bool                `json:"passed"`
	StartedAt     time.Time           `json:"started_at"`
	FinishedAt    time.Time           `json:"finished_at"`
	OutputDir     string              `json:"output_dir"`
	SandboxRoot   string              `json:"sandbox_root"`
	ClientBuild   buildinfo.Info      `json:"client_build"`
	DaemonBuild   buildinfo.Info      `json:"daemon_build"`
	Protocol      int                 `json:"protocol"`
	Steps         []StepResult        `json:"steps"`
	Assertions    []AssertionResult   `json:"assertions"`
	Participants  []ParticipantResult `json:"participants"`
	Processes     []ProcessEvent      `json:"processes"`
	Resources     ResourceUsage       `json:"resources"`
	Error         string              `json:"error,omitempty"`
	Environment   EnvironmentManifest `json:"environment"`
}

type participantState struct {
	definition Participant
	client     *api.Client
	actor      string
	firstActor string
	runID      string
	generation int
}

type execution struct {
	ctx          context.Context
	sandbox      Sandbox
	supervisor   *Supervisor
	hollerd      string
	participants map[string]*participantState
	messages     map[string]bus.SendResult
	claims       map[string]bus.Claim
	acked        map[string]map[string]bool
	steps        []StepResult
	messageCount int
}

func Run(ctx context.Context, config Config) (Report, error) {
	started := time.Now().UTC()
	if strings.TrimSpace(config.HollerdBinary) == "" {
		return Report{}, fmt.Errorf("hollerd binary is required")
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	sandbox, manifest, err := NewSandbox(config.OutputDir)
	if err != nil {
		return Report{}, err
	}
	report := Report{
		SchemaVersion: 1, Scenario: config.Scenario.Name, Description: config.Scenario.Description,
		StartedAt: started, OutputDir: sandbox.OutputDir, SandboxRoot: sandbox.Root,
		ClientBuild: buildinfo.Current(), Protocol: api.ProtocolVersion, Environment: manifest,
	}
	if err := writeJSONFile(filepath.Join(sandbox.EvidenceDir, "environment.json"), manifest); err != nil {
		_ = os.RemoveAll(sandbox.Root)
		return report, err
	}
	if err := writeRedactedScenario(filepath.Join(sandbox.EvidenceDir, "scenario.json"), config.Scenario); err != nil {
		_ = os.RemoveAll(sandbox.Root)
		return report, err
	}

	supervisor := &Supervisor{}
	stdoutPath, stderrPath := daemonLogPaths(sandbox.EvidenceDir)
	if err := supervisor.Start("hollerd", config.HollerdBinary,
		[]string{"--db", sandbox.DatabasePath, "--socket", sandbox.SocketPath, "--codex-binary", filepath.Join(sandbox.Root, "missing-codex")},
		sandbox.Environment, stdoutPath, stderrPath); err != nil {
		report.Error = err.Error()
		return finishReport(report, sandbox, supervisor, config.KeepSandbox, config.IncludeDatabase, err)
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	err = supervisor.WaitForDaemon(readyCtx, sandbox.SocketPath)
	readyCancel()
	if err != nil {
		report.Error = err.Error()
		return finishReport(report, sandbox, supervisor, config.KeepSandbox, config.IncludeDatabase, err)
	}
	probe, probeErr := api.Dial(ctx, sandbox.SocketPath, api.Identity{Actor: "lab-probe", RunID: "lab-probe", Client: "lab/1"})
	if probeErr == nil {
		if info, infoErr := probe.DaemonInfo(ctx); infoErr == nil {
			report.DaemonBuild, report.Protocol = info.Build, info.Protocol
		}
		_ = probe.Close()
	}

	execution := &execution{
		ctx: ctx, sandbox: sandbox, supervisor: supervisor, hollerd: config.HollerdBinary,
		participants: make(map[string]*participantState),
		messages:     make(map[string]bus.SendResult), claims: make(map[string]bus.Claim), acked: make(map[string]map[string]bool),
	}
	for _, participant := range config.Scenario.Participants {
		copy := participant
		execution.participants[participant.ID] = &participantState{definition: copy}
	}
	for index, step := range config.Scenario.Steps {
		stepErr := execution.runStep(index+1, step)
		if stepErr != nil {
			err = stepErr
			break
		}
	}
	report.Steps = execution.steps
	report.Assertions = execution.liveAssertions(config.Scenario.Assertions, manifest)
	if err == nil {
		for _, assertion := range report.Assertions {
			if !assertion.Passed {
				err = fmt.Errorf("assertion %s failed: %s", assertion.Name, assertion.Detail)
				break
			}
		}
	}
	if evidenceErr := execution.writeEventEvidence(); evidenceErr != nil && err == nil {
		err = fmt.Errorf("write event evidence: %w", evidenceErr)
	}
	for _, definition := range config.Scenario.Participants {
		participant := execution.participants[definition.ID]
		report.Participants = append(report.Participants, ParticipantResult{
			ID: participant.definition.ID, Harness: participant.definition.Harness,
			RequestedActor: participant.definition.Actor, AssignedActor: participant.actor,
			RunID: participant.runID, Generations: participant.generation,
		})
		if participant.client != nil {
			_ = participant.client.Close()
			participant.client = nil
		}
	}
	report.Resources.Messages = execution.messageCount
	report.Resources.ModelTokens = 0
	finished, finishErr := finishReport(report, sandbox, supervisor, config.KeepSandbox, config.IncludeDatabase, err)
	if finishErr != nil {
		return finished, finishErr
	}
	return finished, err
}

func (e *execution) runStep(index int, step Step) error {
	result := StepResult{Index: index, Action: step.Action, At: time.Now().UTC()}
	err := e.execute(step)
	result.Passed = err == nil
	if err != nil {
		result.Detail = err.Error()
	}
	e.steps = append(e.steps, result)
	if err != nil {
		return fmt.Errorf("step %d %s: %w", index, step.Action, err)
	}
	return nil
}

func (e *execution) execute(step Step) error {
	switch step.Action {
	case "start":
		return e.start(step.Participant)
	case "start_expect_error":
		err := e.start(step.Participant)
		if err == nil {
			state, _ := e.participant(step.Participant)
			if state != nil && state.client != nil {
				_ = state.client.Close()
				state.client = nil
			}
			return fmt.Errorf("participant %q started successfully; expected an error", step.Participant)
		}
		if step.ErrorContains != "" && !strings.Contains(err.Error(), step.ErrorContains) {
			return fmt.Errorf("start error %q does not contain %q", err, step.ErrorContains)
		}
		return nil
	case "stop":
		state, err := e.participant(step.Participant)
		if err != nil || state.client == nil {
			return firstError(err, fmt.Errorf("participant %q is not running", step.Participant))
		}
		_ = state.client.ExpireRegistration(e.ctx, state.actor, state.runID, state.definition.ID+"-session", "lab-stop")
		err = state.client.Close()
		state.client = nil
		return err
	case "crash":
		state, err := e.participant(step.Participant)
		if err != nil || state.client == nil {
			return firstError(err, fmt.Errorf("participant %q is not running", step.Participant))
		}
		err = state.client.Close()
		state.client = nil
		return err
	case "resume":
		return e.start(step.Participant)
	case "send":
		from, err := e.runningParticipant(step.From)
		if err != nil {
			return err
		}
		to := step.To
		if target, exists := e.participants[to]; exists {
			if target.actor != "" {
				to = target.actor
			} else {
				to = target.definition.Actor
			}
		}
		messageType := step.Type
		if messageType == "" {
			messageType = "MESSAGE"
		}
		body, _ := json.Marshal(map[string]string{"text": step.Body})
		request := bus.SendRequest{
			IdempotencyKey: fmt.Sprintf("lab-%s-%d-%s", safeKey(e.sandbox.OutputDir), len(e.messages)+1, step.SaveAs),
			ProjectID:      "lab", ChannelID: "direct", ThreadID: step.ThreadID, ToActors: []string{to}, Type: messageType,
			DeliveryRequest: bus.DeliveryNonBlocking, Body: body,
		}
		if parent, exists := e.messages[step.Message]; exists {
			request.ThreadID = parent.Message.ThreadID
			request.InReplyTo = parent.Message.ID
		}
		sent, err := from.client.Send(e.ctx, request)
		if err != nil {
			return err
		}
		if step.SaveAs == "" {
			return fmt.Errorf("send save_as is required")
		}
		e.messages[step.SaveAs] = sent
		e.acked[sent.Message.ID] = make(map[string]bool)
		e.messageCount++
		return nil
	case "claim":
		state, err := e.runningParticipant(step.Participant)
		if err != nil {
			return err
		}
		message, exists := e.messages[step.Message]
		if !exists {
			return fmt.Errorf("unknown message %q", step.Message)
		}
		claim, err := state.client.Claim(e.ctx, state.actor, message.Message.ID, time.Minute)
		if err != nil {
			return err
		}
		if step.SaveAs == "" {
			return fmt.Errorf("claim save_as is required")
		}
		e.claims[step.SaveAs] = claim
		return nil
	case "ack":
		state, err := e.runningParticipant(step.Participant)
		if err != nil {
			return err
		}
		message, messageExists := e.messages[step.Message]
		claim, claimExists := e.claims[step.Claim]
		if !messageExists || !claimExists {
			return fmt.Errorf("unknown message %q or claim %q", step.Message, step.Claim)
		}
		if err := state.client.Ack(e.ctx, state.actor, message.Message.ID, claim.LeaseToken); err != nil {
			return err
		}
		e.acked[message.Message.ID][state.actor] = true
		if claim.OriginalRecipientActor != "" {
			e.acked[message.Message.ID][claim.OriginalRecipientActor] = true
		}
		return nil
	case "alias_set":
		operator, err := e.runningParticipant(step.Participant)
		if err != nil {
			return err
		}
		target, err := e.participant(step.Target)
		if err != nil || target.actor == "" {
			return firstError(err, fmt.Errorf("alias target %q has no actor", step.Target))
		}
		_, err = operator.client.SetAlias(e.ctx, bus.AliasSetRequest{
			Alias: step.Alias, Actor: target.actor, ProjectID: "lab",
			IdempotencyKey: fmt.Sprintf("lab-alias-%s-%s", step.Alias, target.actor),
		})
		return err
	case "adopt":
		adopter, err := e.runningParticipant(step.Participant)
		if err != nil {
			return err
		}
		source, err := e.participant(step.Source)
		if err != nil {
			return err
		}
		sourceActor := source.actor
		if sourceActor == "" {
			sourceActor = source.definition.Actor
		}
		_, err = adopter.client.AdoptActor(e.ctx, bus.AdoptRequest{
			SourceActor: sourceActor, ProjectID: "lab",
			IdempotencyKey: fmt.Sprintf("lab-adopt-%s-%s", sourceActor, adopter.actor),
		})
		return err
	case "daemon_restart":
		stopCtx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
		err := e.supervisor.Stop(stopCtx, "hollerd")
		cancel()
		if err != nil {
			return err
		}
		stdoutPath, stderrPath := daemonLogPaths(e.sandbox.EvidenceDir)
		if err := e.supervisor.Start("hollerd", e.hollerd,
			[]string{"--db", e.sandbox.DatabasePath, "--socket", e.sandbox.SocketPath, "--codex-binary", filepath.Join(e.sandbox.Root, "missing-codex")},
			e.sandbox.Environment, stdoutPath, stderrPath); err != nil {
			return err
		}
		readyCtx, readyCancel := context.WithTimeout(e.ctx, 5*time.Second)
		err = e.supervisor.WaitForDaemon(readyCtx, e.sandbox.SocketPath)
		readyCancel()
		if err != nil {
			return err
		}
		for _, state := range e.participants {
			if state.client == nil {
				continue
			}
			_ = state.client.Close()
			client, dialErr := api.Dial(e.ctx, e.sandbox.SocketPath, api.Identity{
				Actor: state.actor, RunID: state.runID, Client: state.definition.Harness + "/1",
				NameMode: bus.NameModeExact, ContinuityHandles: state.definition.Continuity, ProjectID: "lab",
			})
			if dialErr != nil {
				state.client = nil
				return fmt.Errorf("reconnect participant %s after daemon restart: %w", state.definition.ID, dialErr)
			}
			state.client = client
		}
		return nil
	case "assert_inbox":
		state, err := e.runningParticipant(step.Participant)
		if err != nil {
			return err
		}
		items, err := state.client.CheckInbox(e.ctx, state.actor, 100)
		if err != nil {
			return err
		}
		if len(items) != step.Count {
			return fmt.Errorf("participant %q inbox count = %d, want %d", step.Participant, len(items), step.Count)
		}
		return nil
	case "assert_actor_distinct":
		left, err := e.participant(step.Participant)
		if err != nil {
			return err
		}
		right, err := e.participant(step.Target)
		if err != nil {
			return err
		}
		if left.actor == "" || left.actor == right.actor {
			return fmt.Errorf("actors are not distinct: %q and %q", left.actor, right.actor)
		}
		return nil
	case "assert_actor_same":
		state, err := e.participant(step.Participant)
		if err != nil {
			return err
		}
		if state.firstActor == "" || state.actor != state.firstActor {
			return fmt.Errorf("actor after resume = %q, first actor = %q", state.actor, state.firstActor)
		}
		return nil
	case "assert_original_recipient":
		claim, exists := e.claims[step.Claim]
		if !exists {
			return fmt.Errorf("unknown claim %q", step.Claim)
		}
		source, err := e.participant(step.Source)
		if err != nil {
			return err
		}
		want := source.actor
		if want == "" {
			want = source.definition.Actor
		}
		if claim.OriginalRecipientActor != want {
			return fmt.Errorf("original recipient = %q, want %q", claim.OriginalRecipientActor, want)
		}
		return nil
	default:
		return fmt.Errorf("unsupported action %q", step.Action)
	}
}

func (e *execution) start(id string) error {
	state, err := e.participant(id)
	if err != nil {
		return err
	}
	if state.client != nil {
		return fmt.Errorf("participant %q is already running", id)
	}
	state.generation++
	state.runID = fmt.Sprintf("lab-%s-run-%d", id, state.generation)
	identity := api.Identity{
		Actor: state.definition.Actor, RunID: state.runID, Client: state.definition.Harness + "/1",
		NameMode: bus.NameMode(state.definition.NameMode), ContinuityHandles: state.definition.Continuity, ProjectID: "lab",
	}
	client, err := api.Dial(e.ctx, e.sandbox.SocketPath, identity)
	if err != nil {
		return err
	}
	state.client = client
	state.actor = client.Identity().Actor
	if state.firstActor == "" {
		state.firstActor = state.actor
	}
	_, err = client.RegisterSession(e.ctx, bus.RegistrationRequest{
		Actor: state.actor, RunID: state.runID, Harness: "test",
		AttentionMode: "startup-only", SessionID: state.definition.ID + "-session",
		ProjectID: "lab", Lease: 10 * time.Minute,
	})
	if err != nil {
		_ = client.Close()
		state.client = nil
		return err
	}
	return nil
}

func (e *execution) participant(id string) (*participantState, error) {
	state, exists := e.participants[id]
	if !exists {
		return nil, fmt.Errorf("unknown participant %q", id)
	}
	return state, nil
}

func (e *execution) runningParticipant(id string) (*participantState, error) {
	state, err := e.participant(id)
	if err != nil {
		return nil, err
	}
	if state.client == nil {
		return nil, fmt.Errorf("participant %q is not running", id)
	}
	return state, nil
}

func (e *execution) liveAssertions(names []string, manifest EnvironmentManifest) []AssertionResult {
	results := make([]AssertionResult, 0, len(names))
	for _, name := range names {
		result := AssertionResult{Name: name, Passed: true}
		switch name {
		case "every_message_acked":
			for _, message := range e.messages {
				for _, actor := range message.Message.ToActors {
					if !e.acked[message.Message.ID][actor] {
						result.Passed, result.Detail = false, "message "+message.Message.ID+" was not acknowledged by "+actor
					}
				}
			}
		case "durable_inboxes_empty":
			for _, participant := range e.participants {
				if participant.client == nil {
					continue
				}
				items, err := participant.client.CheckInbox(e.ctx, participant.actor, 100)
				if err != nil || len(items) != 0 {
					result.Passed = false
					result.Detail = fmt.Sprintf("actor %s has %d inbox rows: %v", participant.actor, len(items), err)
				}
			}
		case "sandbox_paths_isolated":
			result.Passed = manifest.Isolated
			if !result.Passed {
				result.Detail = "sandbox paths overlap host state"
			}
		case "orphan_processes_zero", "socket_removed":
			// Evaluated after the supervisor has stopped.
			result.Detail = "pending teardown"
		}
		results = append(results, result)
	}
	return results
}

func finishReport(report Report, sandbox Sandbox, supervisor *Supervisor, keepSandbox, includeDatabase bool, runErr error) (Report, error) {
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	stopErrors := supervisor.StopAll(stopCtx)
	stopCancel()
	if len(stopErrors) > 0 && runErr == nil {
		runErr = errors.Join(stopErrors...)
	}
	orphans := supervisor.OrphanPIDs()
	socketRemoved := false
	for attempt := 0; attempt < 20; attempt++ {
		_, statErr := os.Lstat(sandbox.SocketPath)
		if errors.Is(statErr, os.ErrNotExist) {
			socketRemoved = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for index := range report.Assertions {
		switch report.Assertions[index].Name {
		case "orphan_processes_zero":
			report.Assertions[index].Passed = len(orphans) == 0
			report.Assertions[index].Detail = fmt.Sprintf("orphan_pids=%v", orphans)
		case "socket_removed":
			report.Assertions[index].Passed = socketRemoved
			report.Assertions[index].Detail = "socket_removed=" + fmt.Sprint(socketRemoved)
		}
		if !report.Assertions[index].Passed && runErr == nil {
			runErr = fmt.Errorf("assertion %s failed: %s", report.Assertions[index].Name, report.Assertions[index].Detail)
		}
	}
	report.Processes = supervisor.Events()
	report.Resources.Processes = len(report.Processes) / 2
	report.FinishedAt = time.Now().UTC()
	report.Passed = runErr == nil
	if runErr != nil {
		report.Error = runErr.Error()
	}
	if includeDatabase {
		if copyErr := copyFile(sandbox.DatabasePath, filepath.Join(sandbox.EvidenceDir, "holler.sqlite3")); copyErr != nil && !errors.Is(copyErr, os.ErrNotExist) && runErr == nil {
			runErr = copyErr
			report.Passed = false
			report.Error = copyErr.Error()
		}
	}
	_ = writeJSONFile(filepath.Join(sandbox.EvidenceDir, "processes.json"), report.Processes)
	_ = writeJSONFile(filepath.Join(sandbox.EvidenceDir, "assertions.json"), report.Assertions)
	_ = writeJSONFile(filepath.Join(sandbox.EvidenceDir, "report.json"), report)
	_ = writeJUnit(filepath.Join(sandbox.EvidenceDir, "junit.xml"), report)
	if !keepSandbox {
		_ = os.RemoveAll(sandbox.Root)
	}
	return report, runErr
}

func writeRedactedScenario(path string, scenario Scenario) error {
	redacted := scenario
	redacted.Steps = append([]Step(nil), scenario.Steps...)
	for index := range redacted.Steps {
		if redacted.Steps[index].Body != "" {
			redacted.Steps[index].Body = "[redacted]"
		}
	}
	return writeJSONFile(path, redacted)
}

func writeJSONFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func copyFile(source, target string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	return errors.Join(copyErr, closeErr)
}

type junitSuite struct {
	XMLName  xml.Name    `xml:"testsuite"`
	Name     string      `xml:"name,attr"`
	Tests    int         `xml:"tests,attr"`
	Failures int         `xml:"failures,attr"`
	Cases    []junitCase `xml:"testcase"`
}

type junitCase struct {
	Name    string        `xml:"name,attr"`
	Failure *junitFailure `xml:"failure,omitempty"`
}

type junitFailure struct {
	Message string `xml:"message,attr"`
}

func writeJUnit(path string, report Report) error {
	suite := junitSuite{Name: report.Scenario, Tests: len(report.Assertions)}
	for _, assertion := range report.Assertions {
		item := junitCase{Name: assertion.Name}
		if !assertion.Passed {
			suite.Failures++
			item.Failure = &junitFailure{Message: assertion.Detail}
		}
		suite.Cases = append(suite.Cases, item)
	}
	raw, err := xml.MarshalIndent(suite, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append([]byte(xml.Header), raw...), 0o600)
}

func firstError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

func (e *execution) writeEventEvidence() error {
	var client *api.Client
	for _, participant := range e.participants {
		if participant.client != nil {
			client = participant.client
			break
		}
	}
	if client == nil {
		return nil
	}
	result := make([]EventEvidence, 0)
	for _, stream := range []string{"durable", "operational"} {
		events, err := client.ListEvents(e.ctx, "lab", stream, 0, 5000)
		if err != nil {
			return err
		}
		for _, event := range events {
			result = append(result, EventEvidence{
				ID: event.ID, PartitionID: event.PartitionID, Stream: event.Stream,
				Position: event.Position, Kind: event.Kind, MessageID: event.MessageID,
				ActorID: event.ActorID, CreatedAt: event.CreatedAt,
			})
		}
	}
	return writeJSONFile(filepath.Join(e.sandbox.EvidenceDir, "events.json"), result)
}

func safeKey(value string) string {
	value = filepath.Base(filepath.Clean(value))
	var builder strings.Builder
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			builder.WriteRune(character)
		}
	}
	if builder.Len() == 0 {
		return "run"
	}
	return builder.String()
}
