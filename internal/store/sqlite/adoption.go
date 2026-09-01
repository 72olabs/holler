package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

// AdoptActor records one durable winner for an inactive actor's inbox. The
// delivery rows and message recipients remain unchanged; inbox reads resolve
// the forwarding record and report the original recipient explicitly.
func (s *Store) AdoptActor(ctx context.Context, request bus.AdoptRequest) (bus.AdoptResult, error) {
	req, err := normalizeAdoptRequest(request)
	if err != nil {
		return bus.AdoptResult{}, err
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.AdoptResult{}, fmt.Errorf("begin actor adoption: %w", err)
	}
	defer tx.Rollback()

	if previous, found, err := actorAdoptionTx(ctx, tx, req.SourceActor); err != nil {
		return bus.AdoptResult{}, err
	} else if found {
		if previous.AdoptingActor == req.AdoptingActor && previous.IdempotencyKey == req.IdempotencyKey {
			previous.DuplicateRequest = true
			return previous, nil
		}
		return bus.AdoptResult{}, bus.ErrAdoptionConflict
	}
	var reusedSource string
	err = tx.QueryRowContext(ctx, `
		SELECT source_actor FROM actor_adoptions
		WHERE adopting_actor = ? AND idempotency_key = ?`, req.AdoptingActor, req.IdempotencyKey).Scan(&reusedSource)
	if err == nil && reusedSource != req.SourceActor {
		return bus.AdoptResult{}, bus.ErrIdempotencyConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return bus.AdoptResult{}, fmt.Errorf("inspect adoption idempotency: %w", err)
	}
	var sourceAlreadyAdopts, targetAlreadyAdopted int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_adoptions WHERE adopting_actor = ?)`, req.SourceActor).Scan(&sourceAlreadyAdopts); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("inspect source adoption chain: %w", err)
	}
	if sourceAlreadyAdopts != 0 {
		return bus.AdoptResult{}, &bus.ValidationError{Field: "source_actor", Problem: "already receives an adopted inbox; chained adoption is unsupported"}
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_adoptions WHERE source_actor = ?)`, req.AdoptingActor).Scan(&targetAlreadyAdopted); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("inspect target adoption chain: %w", err)
	}
	if targetAlreadyAdopted != 0 {
		return bus.AdoptResult{}, &bus.ValidationError{Field: "adopting_actor", Problem: "was already adopted; chained adoption is unsupported"}
	}

	liveSource, err := actorHasLivePresenceTx(ctx, tx, req.SourceActor, now)
	if err != nil {
		return bus.AdoptResult{}, err
	}
	if liveSource {
		return bus.AdoptResult{}, bus.ErrActorLive
	}
	liveTarget, err := actorHasLivePresenceTx(ctx, tx, req.AdoptingActor, now)
	if err != nil {
		return bus.AdoptResult{}, err
	}
	if !liveTarget {
		return bus.AdoptResult{}, bus.ErrActorNotLive
	}
	liveTargetRun, err := actorRunHasLivePresenceTx(ctx, tx, req.AdoptingActor, req.AdoptingRun, now)
	if err != nil {
		return bus.AdoptResult{}, err
	}
	if !liveTargetRun {
		return bus.AdoptResult{}, bus.ErrRunNotLive
	}

	var activeClaims int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM deliveries d JOIN messages m ON m.message_id = d.message_id
		WHERE d.recipient_actor = ? AND d.state = ? AND d.lease_expires_at_ns > ?
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)`, req.SourceActor,
		bus.DeliveryClaimed, now.UnixNano(), now.UnixNano()).Scan(&activeClaims); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("inspect active source claims: %w", err)
	}
	if activeClaims != 0 {
		return bus.AdoptResult{}, bus.ErrAdoptionBusy
	}
	var transferred, deduplicated int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN EXISTS (
			SELECT 1 FROM deliveries target
			WHERE target.message_id = d.message_id AND target.recipient_actor = ?
		) THEN 1 ELSE 0 END), 0)
		FROM deliveries d JOIN messages m ON m.message_id = d.message_id
		WHERE d.recipient_actor = ? AND d.state IN (?, ?)
		  AND (d.state = ? OR d.lease_expires_at_ns <= ?)
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)`, req.AdoptingActor, req.SourceActor,
		bus.DeliveryQueued, bus.DeliveryClaimed, bus.DeliveryQueued, now.UnixNano(), now.UnixNano()).
		Scan(&transferred, &deduplicated); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("count adoptable deliveries: %w", err)
	}
	if transferred == 0 {
		return bus.AdoptResult{}, bus.ErrNoMessage
	}
	transferred -= deduplicated

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO actor_adoptions(
			source_actor, adopting_actor, adopting_run, project_id, idempotency_key,
			transferred_count, deduplicated_count, adopted_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, req.SourceActor, req.AdoptingActor, req.AdoptingRun,
		req.ProjectID, req.IdempotencyKey, transferred, deduplicated, now.UnixNano()); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("record actor adoption: %w", err)
	}
	// Existing wakes for the source become target wakes. If the target already
	// has a job for the same message, retire the duplicate source job first.
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox AS source
		SET state = 'done', available_at_ns = ?, last_error = 'deduplicated during actor adoption'
		WHERE source.recipient_actor = ? AND EXISTS (
			SELECT 1 FROM notification_outbox target
			WHERE target.message_id = source.message_id AND target.recipient_actor = ?
		)`, now.UnixNano(), req.SourceActor, req.AdoptingActor); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("deduplicate adopted notifications: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET recipient_actor = ?, state = CASE WHEN state = 'done' THEN 'done' ELSE 'pending' END,
		    available_at_ns = ?, last_error = NULL
		WHERE recipient_actor = ? AND state != 'done'`, req.AdoptingActor, now.UnixNano(), req.SourceActor); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("retarget adopted notifications: %w", err)
	}
	// A source wake may already be terminal because the old actor was offline or
	// notification retries were exhausted. Adoption to a live actor is a new
	// routing decision, so create one fresh target wake for each still-claimable
	// attention-requesting delivery that has no target job.
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO notification_outbox(
			message_id, recipient_actor, state, available_at_ns, created_at_ns
		)
		SELECT d.message_id, ?, 'pending', ?, ?
		FROM deliveries d JOIN messages m ON m.message_id = d.message_id
		WHERE d.recipient_actor = ? AND d.state IN (?, ?)
		  AND (d.state = ? OR d.lease_expires_at_ns <= ?)
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		  AND m.delivery_request != ?`, req.AdoptingActor, now.UnixNano(), now.UnixNano(), req.SourceActor,
		bus.DeliveryQueued, bus.DeliveryClaimed, bus.DeliveryQueued, now.UnixNano(), now.UnixNano(), bus.DeliveryNonBlocking); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("enqueue adopted notifications: %w", err)
	}
	payload := bus.EventProvenance(ctx, req.AdoptingRun)
	payload["source_actor"] = req.SourceActor
	payload["adopting_actor"] = req.AdoptingActor
	payload["transferred"] = transferred
	payload["deduplicated"] = deduplicated
	payload["idempotency_key"] = req.IdempotencyKey
	if err := s.appendEventTx(ctx, tx, req.ProjectID, "durable", "actor.adopted", "", req.AdoptingActor, payload, now); err != nil {
		return bus.AdoptResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.AdoptResult{}, fmt.Errorf("commit actor adoption: %w", err)
	}
	return bus.AdoptResult{
		SourceActor: req.SourceActor, AdoptingActor: req.AdoptingActor, AdoptingRun: req.AdoptingRun,
		Transferred: transferred, Deduplicated: deduplicated, AdoptedAt: now,
	}, nil
}

func normalizeAdoptRequest(request bus.AdoptRequest) (bus.AdoptRequest, error) {
	request.SourceActor = strings.TrimSpace(request.SourceActor)
	request.AdoptingActor = strings.TrimSpace(request.AdoptingActor)
	request.AdoptingRun = strings.TrimSpace(request.AdoptingRun)
	request.ProjectID = strings.TrimSpace(request.ProjectID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if request.ProjectID == "" {
		request.ProjectID = "default"
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"source_actor", request.SourceActor, 128}, {"adopting_actor", request.AdoptingActor, 128},
		{"adopting_run", request.AdoptingRun, 256}, {"project_id", request.ProjectID, 256},
		{"idempotency_key", request.IdempotencyKey, 256},
	} {
		if field.value == "" {
			return bus.AdoptRequest{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, field.value, field.max); err != nil {
			return bus.AdoptRequest{}, err
		}
	}
	if request.SourceActor == request.AdoptingActor {
		return bus.AdoptRequest{}, &bus.ValidationError{Field: "source_actor", Problem: "must differ from adopting_actor"}
	}
	return request, nil
}

func actorHasLivePresenceTx(ctx context.Context, tx *sql.Tx, actor string, now time.Time) (bool, error) {
	var live int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM registrations
		WHERE actor = ? AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL
		  AND lease_expires_at_ns > ?)`, actor, now.UnixNano()).Scan(&live); err != nil {
		return false, fmt.Errorf("inspect actor liveness: %w", err)
	}
	return live != 0, nil
}

func actorRunHasLivePresenceTx(ctx context.Context, tx *sql.Tx, actor, runID string, now time.Time) (bool, error) {
	var live int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM registrations
		WHERE actor = ? AND run_id = ? AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL
		  AND lease_expires_at_ns > ?)`, actor, runID, now.UnixNano()).Scan(&live); err != nil {
		return false, fmt.Errorf("inspect actor run liveness: %w", err)
	}
	return live != 0, nil
}

func actorAdoptionTx(ctx context.Context, tx *sql.Tx, source string) (bus.AdoptResult, bool, error) {
	var result bus.AdoptResult
	var adoptedNS int64
	err := tx.QueryRowContext(ctx, `
		SELECT source_actor, adopting_actor, adopting_run, idempotency_key,
		       transferred_count, deduplicated_count, adopted_at_ns
		FROM actor_adoptions WHERE source_actor = ?`, source).Scan(
		&result.SourceActor, &result.AdoptingActor, &result.AdoptingRun, &result.IdempotencyKey,
		&result.Transferred, &result.Deduplicated, &adoptedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.AdoptResult{}, false, nil
	}
	if err != nil {
		return bus.AdoptResult{}, false, fmt.Errorf("read actor adoption: %w", err)
	}
	result.AdoptedAt = time.Unix(0, adoptedNS).UTC()
	return result, true, nil
}

func assertActorNotAdoptedTx(ctx context.Context, tx *sql.Tx, actor string) error {
	var adopted int
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM actor_adoptions WHERE source_actor = ?)`, actor).Scan(&adopted); err != nil {
		return fmt.Errorf("inspect actor adoption: %w", err)
	}
	if adopted != 0 {
		return bus.ErrActorAdopted
	}
	return nil
}
