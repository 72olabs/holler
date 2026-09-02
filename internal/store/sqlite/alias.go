package sqlite

import (
	"context"
	"database/sql"
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

func resolveRecipientsTx(ctx context.Context, tx *sql.Tx, requested []string) ([]string, map[string]string, error) {
	seen := make(map[string]struct{}, len(requested))
	resolved := make([]string, 0, len(requested))
	resolutions := make(map[string]string)
	for _, recipient := range requested {
		actor := recipient
		var target string
		err := tx.QueryRowContext(ctx, `SELECT actor FROM actor_aliases WHERE alias = ?`, recipient).Scan(&target)
		if err == nil {
			actor = target
			resolutions[recipient] = actor
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("resolve recipient alias: %w", err)
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
