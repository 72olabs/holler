package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

const aliasActionSet = "set"
const aliasActionRemove = "remove"

func (s *Store) SetAlias(ctx context.Context, request bus.AliasSetRequest) (bus.AliasMutationResult, error) {
	req, err := normalizeAliasSetRequest(request)
	if err != nil {
		return bus.AliasMutationResult{}, err
	}
	return s.mutateAlias(ctx, req.Alias, req.Actor, aliasActionSet, req.UpdatedByActor,
		req.UpdatedByRun, req.ProjectID, req.IdempotencyKey)
}

// ClaimAliasIfAbsent atomically installs an operator-configured route without
// ever repointing an existing alias. Both winning and losing outcomes are
// durably idempotent so retry timing cannot change the decision.
func (s *Store) ClaimAliasIfAbsent(ctx context.Context, request bus.AliasClaimRequest) (bus.AliasClaimResult, error) {
	req, err := normalizeAliasClaimRequest(request)
	if err != nil {
		return bus.AliasClaimResult{}, err
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.AliasClaimResult{}, fmt.Errorf("begin alias claim: %w", err)
	}
	defer tx.Rollback()

	var storedAlias, storedActor, storedPolicy, storedHarness, storedProject string
	var encoded []byte
	err = tx.QueryRowContext(ctx, `
		SELECT alias, actor, policy_id, harness, project_id, result_json
		FROM actor_alias_claim_requests
		WHERE updated_by_actor = ? AND idempotency_key = ?`, req.UpdatedByActor, req.IdempotencyKey).
		Scan(&storedAlias, &storedActor, &storedPolicy, &storedHarness, &storedProject, &encoded)
	if err == nil {
		if storedAlias != req.Alias || storedActor != req.Actor || storedPolicy != req.PolicyID ||
			storedHarness != req.Harness || storedProject != req.ProjectID {
			return bus.AliasClaimResult{}, bus.ErrIdempotencyConflict
		}
		var result bus.AliasClaimResult
		if err := json.Unmarshal(encoded, &result); err != nil {
			return bus.AliasClaimResult{}, fmt.Errorf("decode idempotent alias claim: %w", err)
		}
		result.DuplicateRequest = true
		return result, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return bus.AliasClaimResult{}, fmt.Errorf("read idempotent alias claim: %w", err)
	}
	if _, err := aliasRequestByIdempotencyTx(ctx, tx, req.UpdatedByActor, req.IdempotencyKey); err == nil {
		return bus.AliasClaimResult{}, bus.ErrIdempotencyConflict
	} else if !errors.Is(err, bus.ErrNotFound) {
		return bus.AliasClaimResult{}, err
	}

	actorExists, err := actorIdentityExistsTx(ctx, tx, req.Actor)
	if err != nil {
		return bus.AliasClaimResult{}, err
	}
	if !actorExists {
		return bus.AliasClaimResult{}, fmt.Errorf("%s: %w", req.Actor, bus.ErrAliasTargetUnknown)
	}
	current, err := aliasByNameTx(ctx, tx, req.Alias)
	result := bus.AliasClaimResult{PolicyID: req.PolicyID}
	switch {
	case err == nil:
		result.Alias = current
	case !errors.Is(err, bus.ErrAliasNotFound):
		return bus.AliasClaimResult{}, err
	default:
		var retired int
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS(SELECT 1 FROM actor_alias_history WHERE alias = ?)`, req.Alias).Scan(&retired); err != nil {
			return bus.AliasClaimResult{}, fmt.Errorf("inspect alias tombstone: %w", err)
		}
		if retired != 0 {
			return bus.AliasClaimResult{}, fmt.Errorf("%s: %w", req.Alias, bus.ErrAliasTombstoned)
		}
		identityExists, err := actorIdentityExistsTx(ctx, tx, req.Alias)
		if err != nil {
			return bus.AliasClaimResult{}, err
		}
		if identityExists {
			return bus.AliasClaimResult{}, fmt.Errorf("%s: %w", req.Alias, bus.ErrAliasConflict)
		}
		claimed := bus.ActorAlias{
			Alias: req.Alias, Actor: req.Actor, Revision: 1, UpdatedByActor: req.UpdatedByActor,
			UpdatedByRun: req.UpdatedByRun, ProjectID: req.ProjectID, UpdatedAt: now,
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO actor_alias_history(alias, revision, actor, action, updated_by_actor, updated_by_run,
				project_id, idempotency_key, updated_at_ns)
			VALUES (?, 1, ?, 'claim', ?, ?, ?, ?, ?)`, req.Alias, req.Actor, req.UpdatedByActor,
			req.UpdatedByRun, req.ProjectID, req.IdempotencyKey, now.UnixNano()); err != nil {
			return bus.AliasClaimResult{}, fmt.Errorf("record alias claim: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO actor_aliases(alias, actor, revision, updated_by_actor, updated_by_run,
				project_id, idempotency_key, updated_at_ns)
			VALUES (?, ?, 1, ?, ?, ?, ?, ?)`, req.Alias, req.Actor, req.UpdatedByActor,
			req.UpdatedByRun, req.ProjectID, req.IdempotencyKey, now.UnixNano()); err != nil {
			return bus.AliasClaimResult{}, fmt.Errorf("claim alias: %w", err)
		}
		result.Alias = claimed
		result.Claimed = true
		if err := s.appendEventTx(ctx, tx, req.ProjectID, "durable", "actor.alias_claim", "", req.UpdatedByActor,
			map[string]interface{}{"alias": req.Alias, "actor": req.Actor, "policy_id": req.PolicyID, "revision": 1}, now); err != nil {
			return bus.AliasClaimResult{}, err
		}
	}

	encoded, err = json.Marshal(result)
	if err != nil {
		return bus.AliasClaimResult{}, fmt.Errorf("encode alias claim result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO actor_alias_claim_requests(updated_by_actor, idempotency_key, alias, actor,
			policy_id, harness, project_id, result_json, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, req.UpdatedByActor, req.IdempotencyKey, req.Alias, req.Actor,
		req.PolicyID, req.Harness, req.ProjectID, encoded, now.UnixNano()); err != nil {
		return bus.AliasClaimResult{}, fmt.Errorf("record alias claim request: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return bus.AliasClaimResult{}, fmt.Errorf("commit alias claim: %w", err)
	}
	return result, nil
}

func (s *Store) RemoveAlias(ctx context.Context, request bus.AliasRemoveRequest) (bus.AliasMutationResult, error) {
	req, err := normalizeAliasRemoveRequest(request)
	if err != nil {
		return bus.AliasMutationResult{}, err
	}
	return s.mutateAlias(ctx, req.Alias, "", aliasActionRemove, req.UpdatedByActor,
		req.UpdatedByRun, req.ProjectID, req.IdempotencyKey)
}

func (s *Store) mutateAlias(ctx context.Context, alias, actor, action, updatedByActor, updatedByRun,
	projectID, idempotencyKey string) (bus.AliasMutationResult, error) {
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.AliasMutationResult{}, fmt.Errorf("begin alias mutation: %w", err)
	}
	defer tx.Rollback()

	previous, err := aliasRequestByIdempotencyTx(ctx, tx, updatedByActor, idempotencyKey)
	if err == nil {
		if previous.Alias != alias || (action == aliasActionSet && previous.Actor != actor) || previous.action != action ||
			previous.ProjectID != projectID {
			return bus.AliasMutationResult{}, bus.ErrIdempotencyConflict
		}
		return bus.AliasMutationResult{Alias: previous.ActorAlias, Removed: action == aliasActionRemove,
			DuplicateRequest: true}, nil
	}
	if !errors.Is(err, bus.ErrNotFound) {
		return bus.AliasMutationResult{}, err
	}
	if exists, err := aliasClaimRequestExistsTx(ctx, tx, updatedByActor, idempotencyKey); err != nil {
		return bus.AliasMutationResult{}, err
	} else if exists {
		return bus.AliasMutationResult{}, bus.ErrIdempotencyConflict
	}

	current, currentErr := aliasByNameTx(ctx, tx, alias)
	if currentErr != nil && !errors.Is(currentErr, bus.ErrAliasNotFound) {
		return bus.AliasMutationResult{}, currentErr
	}
	if action == aliasActionSet {
		if alias == updatedByActor {
			return bus.AliasMutationResult{}, fmt.Errorf("%s: %w", alias, bus.ErrAliasConflict)
		}
		actorExists, err := actorIdentityExistsTx(ctx, tx, actor)
		if err != nil {
			return bus.AliasMutationResult{}, err
		}
		if !actorExists {
			return bus.AliasMutationResult{}, fmt.Errorf("%s: %w", actor, bus.ErrAliasTargetUnknown)
		}
		identityExists, err := actorIdentityExistsTx(ctx, tx, alias)
		if err != nil {
			return bus.AliasMutationResult{}, err
		}
		if identityExists {
			return bus.AliasMutationResult{}, fmt.Errorf("%s: %w", alias, bus.ErrAliasConflict)
		}
	} else if errors.Is(currentErr, bus.ErrAliasNotFound) {
		return bus.AliasMutationResult{}, bus.ErrAliasNotFound
	}

	var revision int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(revision), 0) + 1 FROM actor_alias_history WHERE alias = ?`, alias).
		Scan(&revision); err != nil {
		return bus.AliasMutationResult{}, fmt.Errorf("allocate alias revision: %w", err)
	}
	historyActor := actor
	if action == aliasActionRemove {
		historyActor = current.Actor
	}
	resultAlias := bus.ActorAlias{
		Alias: alias, Actor: historyActor, Revision: revision, UpdatedByActor: updatedByActor,
		UpdatedByRun: updatedByRun, ProjectID: projectID, UpdatedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO actor_alias_history(alias, revision, actor, action, updated_by_actor, updated_by_run,
			project_id, idempotency_key, updated_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, alias, revision, historyActor, action, updatedByActor,
		updatedByRun, projectID, idempotencyKey, now.UnixNano()); err != nil {
		return bus.AliasMutationResult{}, fmt.Errorf("record alias history: %w", err)
	}
	if action == aliasActionSet {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO actor_aliases(alias, actor, revision, updated_by_actor, updated_by_run,
				project_id, idempotency_key, updated_at_ns)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(alias) DO UPDATE SET actor = excluded.actor, revision = excluded.revision,
				updated_by_actor = excluded.updated_by_actor, updated_by_run = excluded.updated_by_run,
				project_id = excluded.project_id, idempotency_key = excluded.idempotency_key,
				updated_at_ns = excluded.updated_at_ns`, alias, actor, revision, updatedByActor,
			updatedByRun, projectID, idempotencyKey, now.UnixNano()); err != nil {
			return bus.AliasMutationResult{}, fmt.Errorf("set alias: %w", err)
		}
	} else if _, err := tx.ExecContext(ctx, `DELETE FROM actor_aliases WHERE alias = ?`, alias); err != nil {
		return bus.AliasMutationResult{}, fmt.Errorf("remove alias: %w", err)
	}
	payload := map[string]interface{}{"alias": alias, "revision": revision, "action": action}
	if historyActor != "" {
		payload["actor"] = historyActor
	}
	if err := s.appendEventTx(ctx, tx, projectID, "durable", "actor.alias_"+action, "", updatedByActor,
		payload, now); err != nil {
		return bus.AliasMutationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.AliasMutationResult{}, fmt.Errorf("commit alias mutation: %w", err)
	}
	return bus.AliasMutationResult{Alias: resultAlias, Removed: action == aliasActionRemove}, nil
}

func (s *Store) ListAliases(ctx context.Context) ([]bus.ActorAlias, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT alias, actor, revision, updated_by_actor, updated_by_run, project_id, updated_at_ns
		FROM actor_aliases ORDER BY alias`)
	if err != nil {
		return nil, fmt.Errorf("list aliases: %w", err)
	}
	defer rows.Close()
	aliases := make([]bus.ActorAlias, 0)
	for rows.Next() {
		var alias bus.ActorAlias
		var updatedAtNS int64
		if err := rows.Scan(&alias.Alias, &alias.Actor, &alias.Revision, &alias.UpdatedByActor,
			&alias.UpdatedByRun, &alias.ProjectID, &updatedAtNS); err != nil {
			return nil, fmt.Errorf("scan alias: %w", err)
		}
		alias.UpdatedAt = time.Unix(0, updatedAtNS).UTC()
		aliases = append(aliases, alias)
	}
	return aliases, rows.Err()
}

func (s *Store) ResolveAlias(ctx context.Context, name string) (bus.ActorAlias, error) {
	name, err := normalizeAlias("alias", name)
	if err != nil {
		return bus.ActorAlias{}, err
	}
	var alias bus.ActorAlias
	var updatedAtNS int64
	err = s.db.QueryRowContext(ctx, `
		SELECT alias, actor, revision, updated_by_actor, updated_by_run, project_id, updated_at_ns
		FROM actor_aliases WHERE alias = ?`, name).Scan(&alias.Alias, &alias.Actor, &alias.Revision,
		&alias.UpdatedByActor, &alias.UpdatedByRun, &alias.ProjectID, &updatedAtNS)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.ActorAlias{}, bus.ErrAliasNotFound
	}
	if err != nil {
		return bus.ActorAlias{}, fmt.Errorf("resolve alias: %w", err)
	}
	alias.UpdatedAt = time.Unix(0, updatedAtNS).UTC()
	return alias, nil
}

func classifyLegacyRoutesTx(ctx context.Context, tx *sql.Tx, requested []string) ([]bus.Route, error) {
	routes := make([]bus.Route, 0, len(requested))
	for _, recipient := range requested {
		kind := bus.RouteActor
		var exists int
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS(SELECT 1 FROM actor_alias_history WHERE alias = ?)`, recipient).Scan(&exists); err != nil {
			return nil, fmt.Errorf("classify legacy recipient: %w", err)
		}
		if exists != 0 {
			kind = bus.RouteAlias
		}
		routes = append(routes, bus.Route{Kind: kind, Value: recipient})
	}
	return routes, nil
}

func resolveRoutesTx(ctx context.Context, tx *sql.Tx, requested []bus.Route) ([]string, map[string]string, error) {
	seen := make(map[string]struct{}, len(requested))
	resolved := make([]string, 0, len(requested))
	resolutions := make(map[string]string)
	for _, route := range requested {
		actor := route.Value
		if route.Kind == bus.RouteAlias {
			var target string
			err := tx.QueryRowContext(ctx, `SELECT actor FROM actor_aliases WHERE alias = ?`, route.Value).Scan(&target)
			if err == nil {
				actor = target
				resolutions[route.Value] = actor
			} else if errors.Is(err, sql.ErrNoRows) {
				var retired int
				if err := tx.QueryRowContext(ctx, `
					SELECT EXISTS(SELECT 1 FROM actor_alias_history WHERE alias = ?)`, route.Value).Scan(&retired); err != nil {
					return nil, nil, fmt.Errorf("inspect recipient alias history: %w", err)
				}
				if retired != 0 {
					return nil, nil, fmt.Errorf("%s: %w", route.Value, bus.ErrAliasTombstoned)
				}
				return nil, nil, fmt.Errorf("%s: %w", route.Value, bus.ErrAliasNotFound)
			} else {
				return nil, nil, fmt.Errorf("resolve recipient alias: %w", err)
			}
		}
		if _, duplicate := seen[actor]; duplicate {
			continue
		}
		seen[actor] = struct{}{}
		resolved = append(resolved, actor)
	}
	sort.Strings(resolved)
	if len(resolutions) == 0 {
		resolutions = nil
	}
	return resolved, resolutions, nil
}

type aliasHistoryRecord struct {
	bus.ActorAlias
	action string
}

func aliasRequestByIdempotencyTx(ctx context.Context, tx *sql.Tx, actor, key string) (aliasHistoryRecord, error) {
	var record aliasHistoryRecord
	var updatedAtNS int64
	err := tx.QueryRowContext(ctx, `
		SELECT alias, actor, revision, action, updated_by_actor, updated_by_run, project_id, updated_at_ns
		FROM actor_alias_history WHERE updated_by_actor = ? AND idempotency_key = ?`, actor, key).Scan(
		&record.Alias, &record.Actor, &record.Revision, &record.action, &record.UpdatedByActor,
		&record.UpdatedByRun, &record.ProjectID, &updatedAtNS)
	if errors.Is(err, sql.ErrNoRows) {
		return aliasHistoryRecord{}, bus.ErrNotFound
	}
	if err != nil {
		return aliasHistoryRecord{}, fmt.Errorf("read idempotent alias mutation: %w", err)
	}
	record.UpdatedAt = time.Unix(0, updatedAtNS).UTC()
	return record, nil
}

func aliasClaimRequestExistsTx(ctx context.Context, tx *sql.Tx, actor, key string) (bool, error) {
	var exists int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM actor_alias_claim_requests
			WHERE updated_by_actor = ? AND idempotency_key = ?
		)`, actor, key).Scan(&exists); err != nil {
		return false, fmt.Errorf("read idempotent alias claim request: %w", err)
	}
	return exists != 0, nil
}

func aliasByNameTx(ctx context.Context, tx *sql.Tx, name string) (bus.ActorAlias, error) {
	var alias bus.ActorAlias
	var updatedAtNS int64
	err := tx.QueryRowContext(ctx, `
		SELECT alias, actor, revision, updated_by_actor, updated_by_run, project_id, updated_at_ns
		FROM actor_aliases WHERE alias = ?`, name).Scan(&alias.Alias, &alias.Actor, &alias.Revision,
		&alias.UpdatedByActor, &alias.UpdatedByRun, &alias.ProjectID, &updatedAtNS)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.ActorAlias{}, bus.ErrAliasNotFound
	}
	if err != nil {
		return bus.ActorAlias{}, fmt.Errorf("read alias: %w", err)
	}
	alias.UpdatedAt = time.Unix(0, updatedAtNS).UTC()
	return alias, nil
}

func normalizeAliasSetRequest(request bus.AliasSetRequest) (bus.AliasSetRequest, error) {
	var err error
	if request.Alias, err = normalizeAlias("alias", request.Alias); err != nil {
		return bus.AliasSetRequest{}, err
	}
	for _, field := range []struct {
		name  string
		value *string
		max   int
	}{
		{"actor", &request.Actor, 128}, {"updated_by_actor", &request.UpdatedByActor, 128},
		{"updated_by_run", &request.UpdatedByRun, 256}, {"project_id", &request.ProjectID, 256},
		{"idempotency_key", &request.IdempotencyKey, 256},
	} {
		*field.value = strings.TrimSpace(*field.value)
		if *field.value == "" {
			return bus.AliasSetRequest{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, *field.value, field.max); err != nil {
			return bus.AliasSetRequest{}, err
		}
	}
	return request, nil
}

func normalizeAliasClaimRequest(request bus.AliasClaimRequest) (bus.AliasClaimRequest, error) {
	var err error
	if request.Alias, err = normalizeAlias("alias", request.Alias); err != nil {
		return bus.AliasClaimRequest{}, err
	}
	for _, field := range []struct {
		name  string
		value *string
		max   int
	}{
		{"actor", &request.Actor, 128}, {"policy_id", &request.PolicyID, 256}, {"harness", &request.Harness, 64},
		{"updated_by_actor", &request.UpdatedByActor, 128}, {"updated_by_run", &request.UpdatedByRun, 256},
		{"project_id", &request.ProjectID, 256}, {"idempotency_key", &request.IdempotencyKey, 256},
	} {
		*field.value = strings.TrimSpace(*field.value)
		if *field.value == "" {
			return bus.AliasClaimRequest{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, *field.value, field.max); err != nil {
			return bus.AliasClaimRequest{}, err
		}
	}
	request.Harness = strings.ToLower(request.Harness)
	if request.PolicyID != "setup:default-workstream-alias" {
		return bus.AliasClaimRequest{}, &bus.ValidationError{Field: "policy_id", Problem: "is not an installed naming policy"}
	}
	switch request.Harness {
	case "claude", "codex", "opencode":
	default:
		return bus.AliasClaimRequest{}, &bus.ValidationError{Field: "harness", Problem: "must be claude, codex, or opencode"}
	}
	if expected := request.ProjectID + "-" + request.Harness; request.Alias != expected {
		return bus.AliasClaimRequest{}, &bus.ValidationError{
			Field: "alias", Problem: fmt.Sprintf("policy %s permits only %q", request.PolicyID, expected),
		}
	}
	return request, nil
}

func normalizeAliasRemoveRequest(request bus.AliasRemoveRequest) (bus.AliasRemoveRequest, error) {
	alias, err := normalizeAlias("alias", request.Alias)
	if err != nil {
		return bus.AliasRemoveRequest{}, err
	}
	request.Alias = alias
	for _, field := range []struct {
		name  string
		value *string
		max   int
	}{
		{"updated_by_actor", &request.UpdatedByActor, 128}, {"updated_by_run", &request.UpdatedByRun, 256},
		{"project_id", &request.ProjectID, 256}, {"idempotency_key", &request.IdempotencyKey, 256},
	} {
		*field.value = strings.TrimSpace(*field.value)
		if *field.value == "" {
			return bus.AliasRemoveRequest{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, *field.value, field.max); err != nil {
			return bus.AliasRemoveRequest{}, err
		}
	}
	return request, nil
}

func normalizeAlias(field, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", &bus.ValidationError{Field: field, Problem: "is required"}
	}
	if err := bus.ValidateTextIdentifier(field, value, 128); err != nil {
		return "", err
	}
	return value, nil
}
