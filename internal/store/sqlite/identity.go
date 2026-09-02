package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/72olabs/holler/internal/bus"
)

const maximumContinuityHandles = 8

// BindActor resolves an explicit naming request before the API connection is
// stamped with its immutable actor identity. Allocation, continuity binding,
// supersession, and their events share one SQLite transaction.
func (s *Store) BindActor(ctx context.Context, request bus.ActorBindRequest) (bus.ActorBindResult, error) {
	req, err := normalizeActorBindRequest(request)
	if err != nil {
		return bus.ActorBindResult{}, err
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ActorBindResult{}, fmt.Errorf("begin actor binding: %w", err)
	}
	defer tx.Rollback()

	result := bus.ActorBindResult{
		Actor: req.RequestedActor, RequestedActor: req.RequestedActor, NameMode: req.NameMode,
		SupersededPresences: []bus.Registration{},
	}
	switch req.NameMode {
	case bus.NameModeExact:
		if err := assertActorNameNotAliasTx(ctx, tx, req.RequestedActor); err != nil {
			return bus.ActorBindResult{}, err
		}
		if err := assertActorNotAdoptedTx(ctx, tx, req.RequestedActor); err != nil {
			return bus.ActorBindResult{}, err
		}
		presences, err := liveOtherPresencesTx(ctx, tx, req.RequestedActor, req.RunID, now)
		if err != nil {
			return bus.ActorBindResult{}, err
		}
		if len(presences) > 0 && !req.Takeover {
			return bus.ActorBindResult{}, bus.ErrActorLive
		}
		if len(presences) > 0 {
			if err := supersedePresencesTx(ctx, tx, presences, now); err != nil {
				return bus.ActorBindResult{}, err
			}
			result.SupersededPresences = presences
			if err := s.appendEventTx(ctx, tx, req.ProjectID, "operational", "actor.presence_superseded", "", result.Actor,
				map[string]interface{}{"run_id": req.RunID, "reason": "explicit_takeover", "superseded": len(presences)}, now); err != nil {
				return bus.ActorBindResult{}, err
			}
		}
	case bus.NameModeAllocate:
		bindingBase := req.RequestedActor
		boundActor, found, err := resolveBoundActorTx(ctx, tx, req.ContinuityHandles)
		if err != nil {
			return bus.ActorBindResult{}, err
		}
		if found {
			if _, adopted, err := actorAdoptionTx(ctx, tx, boundActor); err != nil {
				return bus.ActorBindResult{}, err
			} else if adopted {
				bindingBase, err = actorBaseTx(ctx, tx, boundActor, req.RequestedActor)
				if err != nil {
					return bus.ActorBindResult{}, err
				}
				provisional := !hasAuthoritativeContinuity(req.ContinuityHandles)
				actor, _, err := s.allocateActorTx(ctx, tx, bindingBase, true, now)
				if err != nil {
					return bus.ActorBindResult{}, err
				}
				result.Actor = actor
				result.Provisional = provisional
				result.AdoptedPredecessor = boundActor
			} else {
				superseded, err := runSupersededForActorTx(ctx, tx, boundActor, req.RunID)
				if err != nil {
					return bus.ActorBindResult{}, err
				}
				if superseded {
					return bus.ActorBindResult{}, bus.ErrBindingStale
				}
				result.Actor = boundActor
				result.ContinuityReclaimed = true
			}
		} else {
			provisional := !hasAuthoritativeContinuity(req.ContinuityHandles)
			actor, _, err := s.allocateActorTx(ctx, tx, req.RequestedActor, true, now)
			if err != nil {
				return bus.ActorBindResult{}, err
			}
			result.Actor = actor
			result.Provisional = provisional
		}
		if result.ContinuityReclaimed {
			bindingBase, err = actorBaseTx(ctx, tx, result.Actor, req.RequestedActor)
			if err != nil {
				return bus.ActorBindResult{}, err
			}
		}
		for _, handle := range req.ContinuityHandles {
			query := `
				INSERT INTO continuity_bindings(handle, actor, base_actor, created_at_ns, updated_at_ns)
				VALUES (?, ?, ?, ?, ?)
				ON CONFLICT(handle) DO UPDATE SET updated_at_ns = excluded.updated_at_ns
				WHERE continuity_bindings.actor = excluded.actor`
			if result.AdoptedPredecessor != "" {
				// resolveBoundActorTx already proved that every presented handle
				// either belongs to this adopted predecessor, is unbound, or is
				// an idle provisional binding that it removed. Repointing cannot
				// steal a third actor's continuity handle.
				query = `
					INSERT INTO continuity_bindings(handle, actor, base_actor, created_at_ns, updated_at_ns)
					VALUES (?, ?, ?, ?, ?)
					ON CONFLICT(handle) DO UPDATE SET
						actor = excluded.actor,
						base_actor = excluded.base_actor,
						updated_at_ns = excluded.updated_at_ns`
			}
			if _, err := tx.ExecContext(ctx, query, handle, result.Actor, bindingBase, now.UnixNano(), now.UnixNano()); err != nil {
				return bus.ActorBindResult{}, fmt.Errorf("bind continuity handle: %w", err)
			}
		}
		if hasAuthoritativeContinuity(req.ContinuityHandles) {
			minted, err := s.finalizeActorAllocationTx(ctx, tx, result.Actor, req.RunID, req.ProjectID,
				continuityHandleKinds(req.ContinuityHandles), now)
			if err != nil {
				return bus.ActorBindResult{}, err
			}
			result.Minted = minted
			result.Provisional = false
		} else if result.ContinuityReclaimed {
			result.Provisional, err = actorProvisionalTx(ctx, tx, result.Actor)
			if err != nil {
				return bus.ActorBindResult{}, err
			}
		}
		if result.ContinuityReclaimed {
			presences, err := liveOtherPresencesTx(ctx, tx, result.Actor, req.RunID, now)
			if err != nil {
				return bus.ActorBindResult{}, err
			}
			if len(presences) > 0 {
				if err := supersedePresencesTx(ctx, tx, presences, now); err != nil {
					return bus.ActorBindResult{}, err
				}
				result.SupersededPresences = presences
				if err := s.appendEventTx(ctx, tx, req.ProjectID, "operational", "actor.presence_superseded", "", result.Actor,
					map[string]interface{}{"run_id": req.RunID, "reason": "continuity_reclaim", "superseded": len(presences)}, now); err != nil {
					return bus.ActorBindResult{}, err
				}
			}
		}
	}
	if !result.Provisional {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO actor_names(actor, first_seen_at_ns) VALUES (?, ?)`,
			result.Actor, now.UnixNano()); err != nil {
			return bus.ActorBindResult{}, fmt.Errorf("reserve bound actor name: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return bus.ActorBindResult{}, fmt.Errorf("commit actor binding: %w", err)
	}
	return result, nil
}

// ReserveActorName makes a legacy connection's canonical actor visible to the
// alias namespace without adding it to discovery or fabricating presence.
func (s *Store) ReserveActorName(ctx context.Context, actor string) error {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	if err := bus.ValidateTextIdentifier("actor", actor, 128); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin actor name reservation: %w", err)
	}
	defer tx.Rollback()
	if err := assertActorNameNotAliasTx(ctx, tx, actor); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO actor_names(actor, first_seen_at_ns) VALUES (?, ?)`,
		actor, s.now().UTC().UnixNano()); err != nil {
		return fmt.Errorf("reserve actor name: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit actor name reservation: %w", err)
	}
	return nil
}

// FinalizeActorAllocation turns a process-only reservation into a visible,
// durable actor immediately before the first actor-specific operation.
func (s *Store) FinalizeActorAllocation(ctx context.Context, actor, runID, projectID string, handles []string) error {
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin actor allocation finalization: %w", err)
	}
	defer tx.Rollback()
	if _, err := s.finalizeActorAllocationTx(ctx, tx, actor, runID, projectID, continuityHandleKinds(handles), now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit actor allocation finalization: %w", err)
	}
	return nil
}

func (s *Store) finalizeActorAllocationTx(ctx context.Context, tx *sql.Tx, actor, runID, projectID string, kinds []string, now time.Time) (bool, error) {
	var base string
	var ordinal int
	var provisional int
	err := tx.QueryRowContext(ctx, `SELECT base_actor, ordinal, provisional FROM actor_allocations WHERE actor = ?`, actor).
		Scan(&base, &ordinal, &provisional)
	if errors.Is(err, sql.ErrNoRows) || provisional == 0 {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read provisional actor allocation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE actor_allocations SET provisional = 0 WHERE actor = ? AND provisional = 1`, actor); err != nil {
		return false, fmt.Errorf("finalize actor allocation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO actor_names(actor, first_seen_at_ns) VALUES (?, ?)`, actor, now.UnixNano()); err != nil {
		return false, fmt.Errorf("reserve finalized actor name: %w", err)
	}
	if err := s.appendEventTx(ctx, tx, projectID, "durable", "actor.minted", "", actor,
		map[string]interface{}{
			"run_id": runID, "base_actor": base, "ordinal": ordinal, "continuity_kinds": kinds,
		}, now); err != nil {
		return false, err
	}
	return true, nil
}

// CurrentActorForContinuity lets a provisional API connection detect that a
// later lifecycle hello reconciled its process handle to a resumed actor.
func (s *Store) CurrentActorForContinuity(ctx context.Context, handles []string) (string, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	actor, found, err := boundActorForHandlesTx(ctx, tx, handles)
	if err != nil {
		return "", err
	}
	if !found {
		return "", bus.ErrNotFound
	}
	return actor, nil
}

func (s *Store) ReleaseProvisionalActor(ctx context.Context, actor string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	idle, err := provisionalActorIdleTx(ctx, tx, actor)
	if err != nil || !idle {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM continuity_bindings WHERE actor = ?`, actor); err != nil {
		return fmt.Errorf("release provisional continuity bindings: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM actor_allocations WHERE actor = ? AND provisional = 1`, actor); err != nil {
		return fmt.Errorf("release provisional actor allocation: %w", err)
	}
	return tx.Commit()
}

func actorBaseTx(ctx context.Context, tx *sql.Tx, actor, fallback string) (string, error) {
	var base string
	err := tx.QueryRowContext(ctx, `
		SELECT base_actor FROM actor_allocations WHERE actor = ?
		UNION ALL
		SELECT base_actor FROM continuity_bindings WHERE actor = ?
		LIMIT 1`, actor, actor).Scan(&base)
	if errors.Is(err, sql.ErrNoRows) {
		return fallback, nil
	}
	if err != nil {
		return "", fmt.Errorf("read actor allocation base: %w", err)
	}
	return base, nil
}

func normalizeActorBindRequest(request bus.ActorBindRequest) (bus.ActorBindRequest, error) {
	request.RequestedActor = strings.TrimSpace(request.RequestedActor)
	request.RunID = strings.TrimSpace(request.RunID)
	request.ProjectID = strings.TrimSpace(request.ProjectID)
	if request.ProjectID == "" {
		request.ProjectID = "default"
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"actor", request.RequestedActor, 128},
		{"run_id", request.RunID, 256},
		{"project_id", request.ProjectID, 256},
	} {
		if field.value == "" {
			return bus.ActorBindRequest{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, field.value, field.max); err != nil {
			return bus.ActorBindRequest{}, err
		}
	}
	if request.NameMode != bus.NameModeExact && request.NameMode != bus.NameModeAllocate {
		return bus.ActorBindRequest{}, &bus.ValidationError{Field: "name_mode", Problem: "must be exact or allocate"}
	}
	if request.NameMode == bus.NameModeExact {
		if len(request.ContinuityHandles) > 0 {
			return bus.ActorBindRequest{}, &bus.ValidationError{Field: "continuity_handles", Problem: "are only valid with allocate mode"}
		}
		return request, nil
	}
	if request.Takeover {
		return bus.ActorBindRequest{}, &bus.ValidationError{Field: "takeover", Problem: "is only valid with exact mode"}
	}
	if len(request.ContinuityHandles) == 0 {
		return bus.ActorBindRequest{}, &bus.ValidationError{Field: "continuity_handles", Problem: "requires at least one handle in allocate mode"}
	}
	if len(request.ContinuityHandles) > maximumContinuityHandles {
		return bus.ActorBindRequest{}, &bus.ValidationError{Field: "continuity_handles", Problem: "exceeds 8 entries"}
	}
	seen := make(map[string]struct{}, len(request.ContinuityHandles))
	handles := make([]string, 0, len(request.ContinuityHandles))
	for _, handle := range request.ContinuityHandles {
		handle = strings.TrimSpace(handle)
		if handle == "" {
			return bus.ActorBindRequest{}, &bus.ValidationError{Field: "continuity_handles", Problem: "contains an empty handle"}
		}
		if err := bus.ValidateTextIdentifier("continuity_handles", handle, 512); err != nil {
			return bus.ActorBindRequest{}, err
		}
		separator := strings.IndexByte(handle, ':')
		if separator <= 0 || separator == len(handle)-1 {
			return bus.ActorBindRequest{}, &bus.ValidationError{Field: "continuity_handles", Problem: "must contain a non-empty namespace and value"}
		}
		switch handle[:separator] {
		case "process", "session", "launch":
		default:
			return bus.ActorBindRequest{}, &bus.ValidationError{Field: "continuity_handles", Problem: "namespace must be process, session, or launch"}
		}
		if _, exists := seen[handle]; exists {
			continue
		}
		seen[handle] = struct{}{}
		handles = append(handles, handle)
	}
	sort.Strings(handles)
	request.ContinuityHandles = handles
	return request, nil
}

func boundActorForHandlesTx(ctx context.Context, tx *sql.Tx, handles []string) (string, bool, error) {
	actor := ""
	for _, handle := range handles {
		var candidate string
		err := tx.QueryRowContext(ctx, `SELECT actor FROM continuity_bindings WHERE handle = ?`, handle).Scan(&candidate)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return "", false, fmt.Errorf("read continuity binding: %w", err)
		}
		if actor != "" && actor != candidate {
			return "", false, bus.ErrContinuityConflict
		}
		actor = candidate
	}
	return actor, actor != "", nil
}

func resolveBoundActorTx(ctx context.Context, tx *sql.Tx, handles []string) (string, bool, error) {
	actors := make(map[string]struct{})
	authoritative := make(map[string]struct{})
	for _, handle := range handles {
		var actor string
		err := tx.QueryRowContext(ctx, `SELECT actor FROM continuity_bindings WHERE handle = ?`, handle).Scan(&actor)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return "", false, fmt.Errorf("read continuity binding: %w", err)
		}
		actors[actor] = struct{}{}
		if continuityHandleKind(handle) != "process" {
			authoritative[actor] = struct{}{}
		}
	}
	if len(actors) == 0 {
		return "", false, nil
	}
	if len(actors) == 1 {
		for actor := range actors {
			return actor, true, nil
		}
	}
	if len(authoritative) != 1 {
		return "", false, bus.ErrContinuityConflict
	}
	var winner string
	for actor := range authoritative {
		winner = actor
	}
	for actor := range actors {
		if actor == winner {
			continue
		}
		idle, err := provisionalActorIdleTx(ctx, tx, actor)
		if err != nil {
			return "", false, err
		}
		if !idle {
			return "", false, bus.ErrContinuityConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM continuity_bindings WHERE actor = ?`, actor); err != nil {
			return "", false, fmt.Errorf("release provisional continuity binding: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM actor_allocations WHERE actor = ? AND provisional = 1`, actor); err != nil {
			return "", false, fmt.Errorf("release provisional actor allocation: %w", err)
		}
	}
	return winner, true, nil
}

func provisionalActorIdleTx(ctx context.Context, tx *sql.Tx, actor string) (bool, error) {
	var idle int
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM actor_allocations a
			WHERE a.actor = ? AND a.provisional = 1
			  AND NOT EXISTS (SELECT 1 FROM continuity_bindings c WHERE c.actor = a.actor AND c.handle NOT LIKE 'process:%')
			  AND NOT EXISTS (SELECT 1 FROM actor_profiles p WHERE p.actor = a.actor)
			  AND NOT EXISTS (SELECT 1 FROM registrations r WHERE r.actor = a.actor)
			  AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.from_actor = a.actor)
			  AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.recipient_actor = a.actor)
		)`, actor).Scan(&idle)
	if err != nil {
		return false, fmt.Errorf("inspect provisional actor allocation: %w", err)
	}
	return idle != 0, nil
}

func actorProvisionalTx(ctx context.Context, tx *sql.Tx, actor string) (bool, error) {
	var provisional int
	err := tx.QueryRowContext(ctx, `SELECT provisional FROM actor_allocations WHERE actor = ?`, actor).Scan(&provisional)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read actor allocation state: %w", err)
	}
	return provisional != 0, nil
}

func (s *Store) allocateActorTx(ctx context.Context, tx *sql.Tx, base string, provisional bool, now time.Time) (string, int, error) {
	const suffixBytes = 6
	for len(base) > 128-suffixBytes-1 {
		_, size := utf8.DecodeLastRuneInString(base)
		base = base[:len(base)-size]
	}
	var firstOrdinal int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(ordinal), 0) + 1 FROM actor_allocations WHERE base_actor = ?`, base).
		Scan(&firstOrdinal); err != nil {
		return "", 0, fmt.Errorf("allocate actor ordinal: %w", err)
	}
	for attempt := 1; attempt <= 256; attempt++ {
		seed, err := s.newID("actor")
		if err != nil {
			return "", 0, fmt.Errorf("generate opaque actor id: %w", err)
		}
		digest := sha256.Sum256([]byte(seed))
		candidate := base + "-" + hex.EncodeToString(digest[:suffixBytes/2])
		ordinal := firstOrdinal + attempt - 1
		if err := bus.ValidateTextIdentifier("allocated_actor", candidate, 128); err != nil {
			return "", 0, err
		}
		used, err := actorExistsTx(ctx, tx, candidate)
		if err != nil {
			return "", 0, err
		}
		if used {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO actor_allocations(actor, base_actor, ordinal, allocated_at_ns, provisional)
			VALUES (?, ?, ?, ?, ?)`, candidate, base, ordinal, now.UnixNano(), boolInt(provisional)); err != nil {
			return "", 0, fmt.Errorf("record actor allocation: %w", err)
		}
		return candidate, ordinal, nil
	}
	return "", 0, errors.New("actor allocation space exhausted")
}

func hasAuthoritativeContinuity(handles []string) bool {
	for _, handle := range handles {
		if continuityHandleKind(handle) != "process" {
			return true
		}
	}
	return false
}

func continuityHandleKind(handle string) string {
	if index := strings.IndexByte(handle, ':'); index >= 0 {
		return handle[:index]
	}
	return handle
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func actorExistsTx(ctx context.Context, tx *sql.Tx, actor string) (bool, error) {
	return actorNameReservedTx(ctx, tx, actor)
}

func actorNameReservedTx(ctx context.Context, tx *sql.Tx, actor string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM actor_allocations WHERE actor = ?
			UNION ALL SELECT 1 FROM actor_profiles WHERE actor = ?
			UNION ALL SELECT 1 FROM registrations WHERE actor = ?
			UNION ALL SELECT 1 FROM messages WHERE from_actor = ?
			UNION ALL SELECT 1 FROM deliveries WHERE recipient_actor = ?
			UNION ALL SELECT 1 FROM actor_alias_history WHERE alias = ?
			UNION ALL SELECT 1 FROM actor_names WHERE actor = ?
			UNION ALL SELECT 1 FROM actor_alias_history WHERE updated_by_actor = ?
		)`, actor, actor, actor, actor, actor, actor, actor, actor).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check actor allocation: %w", err)
	}
	return exists != 0, nil
}

func actorIdentityExistsTx(ctx context.Context, tx *sql.Tx, actor string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM actor_allocations WHERE actor = ?
			UNION ALL SELECT 1 FROM actor_profiles WHERE actor = ?
			UNION ALL SELECT 1 FROM registrations WHERE actor = ?
			UNION ALL SELECT 1 FROM messages WHERE from_actor = ?
			UNION ALL SELECT 1 FROM deliveries WHERE recipient_actor = ?
			UNION ALL SELECT 1 FROM actor_adoptions WHERE source_actor = ? OR adopting_actor = ?
			UNION ALL SELECT 1 FROM actor_alias_history WHERE updated_by_actor = ?
			UNION ALL SELECT 1 FROM actor_names WHERE actor = ?
		)`, actor, actor, actor, actor, actor, actor, actor, actor, actor).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check actor identity: %w", err)
	}
	return exists != 0, nil
}

func assertActorNameNotAliasTx(ctx context.Context, tx *sql.Tx, actor string) error {
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_alias_history WHERE alias = ?)`, actor).Scan(&exists); err != nil {
		return fmt.Errorf("check alias reservation: %w", err)
	}
	if exists != 0 {
		return fmt.Errorf("%s: %w", actor, bus.ErrAliasConflict)
	}
	return nil
}

func runSupersededForActorTx(ctx context.Context, tx *sql.Tx, actor, runID string) (bool, error) {
	var superseded int
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM registrations
			WHERE actor = ? AND run_id = ? AND attention_superseded_at_ns IS NOT NULL
		)`, actor, runID).Scan(&superseded)
	if err != nil {
		return false, fmt.Errorf("check superseded actor binding: %w", err)
	}
	return superseded != 0, nil
}

func liveOtherPresencesTx(ctx context.Context, tx *sql.Tx, actor, runID string, now time.Time) ([]bus.Registration, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT actor, run_id, harness, attention_mode, session_id, delivery_handle, project_id, working_directory,
		       epoch, updated_at_ns, lease_expires_at_ns
		FROM registrations
		WHERE actor = ? AND run_id <> ? AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL
		  AND lease_expires_at_ns > ?
		ORDER BY registered_at_ns DESC, run_id, session_id`, actor, runID, now.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("query competing actor presences: %w", err)
	}
	defer rows.Close()
	presences := make([]bus.Registration, 0)
	for rows.Next() {
		registration, err := scanRegistration(rows)
		if err != nil {
			return nil, err
		}
		presences = append(presences, registration)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate competing actor presences: %w", err)
	}
	return presences, nil
}

func supersedePresencesTx(ctx context.Context, tx *sql.Tx, presences []bus.Registration, now time.Time) error {
	for _, presence := range presences {
		if _, err := tx.ExecContext(ctx, `
			UPDATE registrations
			SET updated_at_ns = ?, lease_expires_at_ns = ?, attention_superseded_at_ns = ?
			WHERE actor = ? AND run_id = ? AND session_id = ?
			  AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL`,
			now.UnixNano(), now.UnixNano(), now.UnixNano(), presence.Actor, presence.RunID, presence.SessionID); err != nil {
			return fmt.Errorf("supersede actor presence: %w", err)
		}
	}
	return nil
}

func continuityHandleKinds(handles []string) []string {
	kinds := make([]string, 0, len(handles))
	seen := make(map[string]struct{}, len(handles))
	for _, handle := range handles {
		kind := handle
		if index := strings.IndexByte(handle, ':'); index >= 0 {
			kind = handle[:index]
		}
		if _, exists := seen[kind]; exists {
			continue
		}
		seen[kind] = struct{}{}
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}
