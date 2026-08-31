package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

const (
	defaultWhoLimit      = 100
	maximumWhoLimit      = 500
	maximumProfileBytes  = 2048
	maximumAccepts       = 32
	maximumAcceptBytes   = 64
	maximumActorSessions = 10
)

// SetActorProfile appends a new descriptive profile revision for an actor.
// Profiles deliberately have no effect on delivery or authorization policy.
func (s *Store) SetActorProfile(ctx context.Context, actor, runID, projectID string, request bus.ActorProfileRequest) (bus.ActorProfileResult, error) {
	actor = strings.TrimSpace(actor)
	runID = strings.TrimSpace(runID)
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		projectID = "default"
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"actor", actor, 128},
		{"run_id", runID, 256},
		{"project_id", projectID, 256},
	} {
		if field.value == "" {
			return bus.ActorProfileResult{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, field.value, field.max); err != nil {
			return bus.ActorProfileResult{}, err
		}
	}
	if strings.TrimSpace(request.RoleText) == "" {
		return bus.ActorProfileResult{}, &bus.ValidationError{Field: "role_text", Problem: "is required"}
	}
	if len(request.RoleText) > maximumProfileBytes {
		return bus.ActorProfileResult{}, &bus.ValidationError{Field: "role_text", Problem: "exceeds 2048 bytes"}
	}
	if err := bus.ValidateTextIdentifier("role_text", request.RoleText, maximumProfileBytes); err != nil {
		return bus.ActorProfileResult{}, err
	}
	accepts, err := normalizeAccepts(request.Accepts)
	if err != nil {
		return bus.ActorProfileResult{}, err
	}
	encodedAccepts, err := json.Marshal(accepts)
	if err != nil {
		return bus.ActorProfileResult{}, fmt.Errorf("encode profile accepts: %w", err)
	}

	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ActorProfileResult{}, fmt.Errorf("begin actor profile update: %w", err)
	}
	defer tx.Rollback()

	current, err := currentActorProfileTx(ctx, tx, actor)
	if err == nil && current.RoleText == request.RoleText && reflect.DeepEqual(current.Accepts, accepts) {
		return bus.ActorProfileResult{Profile: current, Updated: false}, nil
	}
	if err != nil && !errors.Is(err, bus.ErrNotFound) {
		return bus.ActorProfileResult{}, err
	}
	revision := int64(1)
	if err == nil {
		revision = current.Revision + 1
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO actor_profiles(
			actor, revision, role_text, accepts_json, updated_by_run, project_id, updated_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?)`, actor, revision, request.RoleText, encodedAccepts,
		runID, projectID, now.UnixNano()); err != nil {
		return bus.ActorProfileResult{}, fmt.Errorf("insert actor profile: %w", err)
	}
	profile := bus.ActorProfile{
		Actor: actor, RoleText: request.RoleText, Accepts: accepts, Revision: revision,
		UpdatedByRun: runID, UpdatedAt: now,
	}
	payload := bus.EventProvenance(ctx, runID)
	payload["revision"] = revision
	payload["role_text"] = request.RoleText
	payload["accepts"] = accepts
	if err := s.appendEventTx(ctx, tx, projectID, "durable", "actor.profile_updated", "", actor, payload, now); err != nil {
		return bus.ActorProfileResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.ActorProfileResult{}, fmt.Errorf("commit actor profile: %w", err)
	}
	return bus.ActorProfileResult{Profile: profile, Updated: true}, nil
}

func normalizeAccepts(values []string) ([]string, error) {
	if len(values) > maximumAccepts {
		return nil, &bus.ValidationError{Field: "accepts", Problem: "exceeds 32 entries"}
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, &bus.ValidationError{Field: "accepts", Problem: "contains an empty entry"}
		}
		if err := bus.ValidateTextIdentifier("accepts", value, maximumAcceptBytes); err != nil {
			return nil, err
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if result == nil {
		result = []string{}
	}
	sort.Strings(result)
	return result, nil
}

func currentActorProfileTx(ctx context.Context, tx *sql.Tx, actor string) (bus.ActorProfile, error) {
	var profile bus.ActorProfile
	var accepts []byte
	var updatedNS int64
	err := tx.QueryRowContext(ctx, `
		SELECT actor, role_text, accepts_json, revision, updated_by_run, updated_at_ns
		FROM actor_profiles WHERE actor = ? ORDER BY revision DESC LIMIT 1`, actor).
		Scan(&profile.Actor, &profile.RoleText, &accepts, &profile.Revision, &profile.UpdatedByRun, &updatedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.ActorProfile{}, bus.ErrNotFound
	}
	if err != nil {
		return bus.ActorProfile{}, fmt.Errorf("read actor profile: %w", err)
	}
	if err := json.Unmarshal(accepts, &profile.Accepts); err != nil {
		return bus.ActorProfile{}, fmt.Errorf("decode actor profile accepts: %w", err)
	}
	if profile.Accepts == nil {
		profile.Accepts = []string{}
	}
	profile.UpdatedAt = time.Unix(0, updatedNS).UTC()
	return profile, nil
}

// Who returns a bounded, deterministic directory of locally known actors.
// Profile text and all other actor-authored metadata must be treated as
// untrusted descriptive input by callers.
func (s *Store) Who(ctx context.Context, limit int) (bus.ActorDirectory, error) {
	if limit <= 0 {
		limit = defaultWhoLimit
	}
	if limit > maximumWhoLimit {
		limit = maximumWhoLimit
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("begin actor directory snapshot: %w", err)
	}
	defer tx.Rollback()
	entries := make(map[string]*bus.ActorDirectoryEntry)
	entryFor := func(actor string) *bus.ActorDirectoryEntry {
		entry := entries[actor]
		if entry == nil {
			entry = &bus.ActorDirectoryEntry{Actor: actor, State: "unknown", Sessions: []bus.ActorSession{}}
			entries[actor] = entry
		}
		return entry
	}
	allocationRows, err := tx.QueryContext(ctx, `SELECT actor, allocated_at_ns FROM actor_allocations WHERE provisional = 0`)
	if err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("query actor allocations: %w", err)
	}
	for allocationRows.Next() {
		var actor string
		var allocatedNS int64
		if err := allocationRows.Scan(&actor, &allocatedNS); err != nil {
			allocationRows.Close()
			return bus.ActorDirectory{}, fmt.Errorf("scan actor allocation: %w", err)
		}
		entry := entryFor(actor)
		entry.LastSeenAt = laterTime(entry.LastSeenAt, time.Unix(0, allocatedNS).UTC())
	}
	if err := allocationRows.Close(); err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("close actor allocation rows: %w", err)
	}

	profileRows, err := tx.QueryContext(ctx, `
		SELECT p.actor, p.role_text, p.accepts_json, p.revision, p.updated_by_run, p.updated_at_ns
		FROM actor_profiles p
		JOIN (
			SELECT actor, MAX(revision) AS revision FROM actor_profiles GROUP BY actor
		) current ON current.actor = p.actor AND current.revision = p.revision`)
	if err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("query actor profiles: %w", err)
	}
	for profileRows.Next() {
		var profile bus.ActorProfile
		var accepts []byte
		var updatedNS int64
		if err := profileRows.Scan(&profile.Actor, &profile.RoleText, &accepts, &profile.Revision, &profile.UpdatedByRun, &updatedNS); err != nil {
			profileRows.Close()
			return bus.ActorDirectory{}, fmt.Errorf("scan actor profile: %w", err)
		}
		if err := json.Unmarshal(accepts, &profile.Accepts); err != nil {
			profileRows.Close()
			return bus.ActorDirectory{}, fmt.Errorf("decode actor profile accepts: %w", err)
		}
		if profile.Accepts == nil {
			profile.Accepts = []string{}
		}
		profile.UpdatedAt = time.Unix(0, updatedNS).UTC()
		entry := entryFor(profile.Actor)
		entry.Profile = &profile
		entry.LastSeenAt = laterTime(entry.LastSeenAt, profile.UpdatedAt)
	}
	if err := profileRows.Close(); err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("close actor profile rows: %w", err)
	}

	sessionRows, err := tx.QueryContext(ctx, `
		SELECT actor, run_id, harness, attention_mode, project_id, working_directory,
		       registered_at_ns, updated_at_ns, lease_expires_at_ns, ended_at_ns, attention_superseded_at_ns
		FROM registrations
		ORDER BY actor, registered_at_ns DESC, run_id, session_id`)
	if err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("query actor sessions: %w", err)
	}
	for sessionRows.Next() {
		var session bus.ActorSession
		var actor string
		var startedNS sql.NullInt64
		var updatedNS, expiresNS int64
		var endedNS, supersededNS sql.NullInt64
		if err := sessionRows.Scan(&actor, &session.RunID, &session.Harness, &session.AttentionMode,
			&session.ProjectID, &session.WorkingDir, &startedNS, &updatedNS,
			&expiresNS, &endedNS, &supersededNS); err != nil {
			sessionRows.Close()
			return bus.ActorDirectory{}, fmt.Errorf("scan actor session: %w", err)
		}
		startedAtNS := updatedNS
		if startedNS.Valid {
			startedAtNS = startedNS.Int64
		}
		session.StartedAt = time.Unix(0, startedAtNS).UTC()
		session.LastSeenAt = time.Unix(0, updatedNS).UTC()
		session.LeaseExpiresAt = time.Unix(0, expiresNS).UTC()
		switch {
		case endedNS.Valid:
			ended := time.Unix(0, endedNS.Int64).UTC()
			session.EndedAt = &ended
			session.State = "ended"
		case supersededNS.Valid:
			session.State = "superseded"
		case expiresNS <= now.UnixNano():
			session.State = "lapsed"
		default:
			session.State = "live"
		}
		entry := entryFor(actor)
		if session.State == "live" {
			entry.State = "live"
		} else if entry.State == "unknown" {
			if session.State == "ended" {
				entry.State = "ended"
			} else {
				entry.State = "lapsed"
			}
		}
		if len(entry.Sessions) < maximumActorSessions {
			entry.Sessions = append(entry.Sessions, session)
		} else {
			entry.SessionsTruncated = true
		}
		entry.LastSeenAt = laterTime(entry.LastSeenAt, session.LastSeenAt)
	}
	if err := sessionRows.Close(); err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("close actor session rows: %w", err)
	}

	activityRows, err := tx.QueryContext(ctx, `
		WITH activity(actor, at_ns) AS (
			SELECT from_actor, created_at_ns FROM messages
			UNION ALL
			SELECT d.recipient_actor, m.created_at_ns FROM deliveries d JOIN messages m ON m.message_id = d.message_id
			UNION ALL
			SELECT actor, updated_at_ns FROM registrations
			UNION ALL
			SELECT actor, updated_at_ns FROM actor_profiles
		)
		SELECT actor, MAX(at_ns) FROM activity WHERE actor <> '' GROUP BY actor`)
	if err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("query actor activity: %w", err)
	}
	for activityRows.Next() {
		var actor string
		var atNS int64
		if err := activityRows.Scan(&actor, &atNS); err != nil {
			activityRows.Close()
			return bus.ActorDirectory{}, fmt.Errorf("scan actor activity: %w", err)
		}
		entry := entryFor(actor)
		entry.LastSeenAt = laterTime(entry.LastSeenAt, time.Unix(0, atNS).UTC())
	}
	if err := activityRows.Close(); err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("close actor activity rows: %w", err)
	}

	unclaimedRows, err := tx.QueryContext(ctx, `
		SELECT d.recipient_actor, COUNT(*)
		FROM deliveries d JOIN messages m ON m.message_id = d.message_id
		WHERE (d.state = ? OR (d.state = ? AND d.lease_expires_at_ns <= ?))
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		GROUP BY d.recipient_actor`, bus.DeliveryQueued, bus.DeliveryClaimed, now.UnixNano(), now.UnixNano())
	if err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("query actor unclaimed messages: %w", err)
	}
	for unclaimedRows.Next() {
		var actor string
		var count int
		if err := unclaimedRows.Scan(&actor, &count); err != nil {
			unclaimedRows.Close()
			return bus.ActorDirectory{}, fmt.Errorf("scan actor unclaimed messages: %w", err)
		}
		entryFor(actor).UnclaimedMessages = count
	}
	if err := unclaimedRows.Close(); err != nil {
		return bus.ActorDirectory{}, fmt.Errorf("close actor unclaimed rows: %w", err)
	}

	actors := make([]bus.ActorDirectoryEntry, 0, len(entries))
	for _, entry := range entries {
		actors = append(actors, *entry)
	}
	sort.Slice(actors, func(i, j int) bool {
		iRank, jRank := actorStateRank(actors[i].State), actorStateRank(actors[j].State)
		if iRank != jRank {
			return iRank < jRank
		}
		if !actors[i].LastSeenAt.Equal(actors[j].LastSeenAt) {
			return actors[i].LastSeenAt.After(actors[j].LastSeenAt)
		}
		return actors[i].Actor < actors[j].Actor
	})
	directory := bus.ActorDirectory{Actors: actors, GeneratedAt: now, MetadataTrust: "untrusted"}
	if len(directory.Actors) > limit {
		directory.Actors = directory.Actors[:limit]
		directory.Truncated = true
	}
	return directory, nil
}

func actorStateRank(state string) int {
	switch state {
	case "live":
		return 0
	case "ended":
		return 1
	case "lapsed":
		return 2
	default:
		return 3
	}
}

func laterTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
