package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func (s *Store) ObserveCondition(ctx context.Context, observation bus.ConditionObservation) (bus.OperatorCondition, error) {
	observation.Kind = strings.TrimSpace(observation.Kind)
	observation.Subject = strings.TrimSpace(observation.Subject)
	observation.ReasonCode = strings.TrimSpace(observation.ReasonCode)
	observation.Summary = strings.TrimSpace(observation.Summary)
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{{"condition.kind", observation.Kind, 64}, {"condition.subject", observation.Subject, 512},
		{"condition.reason_code", observation.ReasonCode, 128}, {"condition.summary", observation.Summary, 1024}} {
		if field.value == "" {
			return bus.OperatorCondition{}, &bus.ValidationError{Field: field.name, Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier(field.name, field.value, field.max); err != nil {
			return bus.OperatorCondition{}, err
		}
	}
	if len(observation.Details) == 0 {
		observation.Details = json.RawMessage(`{}`)
	}
	if len(observation.Details) > 64<<10 || !json.Valid(observation.Details) {
		return bus.OperatorCondition{}, &bus.ValidationError{Field: "condition.details", Problem: "must be valid JSON no larger than 64 KiB"}
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.OperatorCondition{}, fmt.Errorf("begin condition observation: %w", err)
	}
	defer tx.Rollback()
	var generation int
	var state bus.ConditionState
	var snoozedUntil sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT generation, state, snoozed_until_ns FROM operator_conditions
		WHERE condition_kind = ? AND subject = ?`, observation.Kind, observation.Subject).
		Scan(&generation, &state, &snoozedUntil)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		generation = 1
		state = bus.ConditionActiveVisible
		_, err = tx.ExecContext(ctx, `
			INSERT INTO operator_conditions(
				condition_kind, subject, generation, state, reason_code, summary, details_json,
				first_seen_at_ns, last_seen_at_ns
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			observation.Kind, observation.Subject, generation, state, observation.ReasonCode,
			observation.Summary, []byte(observation.Details), now.UnixNano(), now.UnixNano())
	case err != nil:
		return bus.OperatorCondition{}, fmt.Errorf("read condition observation: %w", err)
	default:
		if state == bus.ConditionResolved {
			generation++
			state = bus.ConditionActiveVisible
		} else if state == bus.ConditionActiveSnoozed && snoozedUntil.Valid && snoozedUntil.Int64 <= now.UnixNano() {
			state = bus.ConditionActiveVisible
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE operator_conditions SET generation = ?, state = ?, reason_code = ?, summary = ?,
				details_json = ?, last_seen_at_ns = ?, resolved_at_ns = NULL,
				snoozed_until_ns = CASE WHEN ? = 'active_snoozed' THEN snoozed_until_ns END,
				acknowledged_at_ns = CASE WHEN ? = 'active_acknowledged' THEN acknowledged_at_ns END,
				presentation_owner = NULL, presentation_lease_until_ns = NULL
			WHERE condition_kind = ? AND subject = ?`, generation, state, observation.ReasonCode,
			observation.Summary, []byte(observation.Details), now.UnixNano(), state, state,
			observation.Kind, observation.Subject)
	}
	if err != nil {
		return bus.OperatorCondition{}, fmt.Errorf("write condition observation: %w", err)
	}
	condition, err := getConditionTx(ctx, tx, observation.Kind, observation.Subject)
	if err != nil {
		return bus.OperatorCondition{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.OperatorCondition{}, fmt.Errorf("commit condition observation: %w", err)
	}
	return condition, nil
}

func (s *Store) ResolveCondition(ctx context.Context, kind, subject string) error {
	kind, subject = strings.TrimSpace(kind), strings.TrimSpace(subject)
	if kind == "" || subject == "" {
		return &bus.ValidationError{Field: "condition", Problem: "kind and subject are required"}
	}
	now := s.now().UTC().UnixNano()
	_, err := s.db.ExecContext(ctx, `
		UPDATE operator_conditions SET state = ?, resolved_at_ns = ?, last_seen_at_ns = ?,
			snoozed_until_ns = NULL,
			presentation_owner = NULL, presentation_lease_until_ns = NULL
		WHERE condition_kind = ? AND subject = ? AND state <> ?`,
		bus.ConditionResolved, now, now, kind, subject, bus.ConditionResolved)
	if err != nil {
		return fmt.Errorf("resolve condition: %w", err)
	}
	return nil
}

func (s *Store) ResolveConditionIfReason(ctx context.Context, kind, subject, reason string) error {
	kind = strings.TrimSpace(kind)
	subject = strings.TrimSpace(subject)
	reason = strings.TrimSpace(reason)
	if kind == "" || subject == "" || reason == "" {
		return &bus.ValidationError{Field: "condition", Problem: "kind, subject, and reason are required"}
	}
	now := s.now().UTC().UnixNano()
	_, err := s.db.ExecContext(ctx, `
		UPDATE operator_conditions SET state = ?, resolved_at_ns = ?, last_seen_at_ns = ?,
			snoozed_until_ns = NULL, presentation_owner = NULL, presentation_lease_until_ns = NULL
		WHERE condition_kind = ? AND subject = ? AND reason_code = ? AND state <> ?`,
		bus.ConditionResolved, now, now, kind, subject, reason, bus.ConditionResolved)
	if err != nil {
		return fmt.Errorf("resolve condition reason: %w", err)
	}
	return nil
}

func (s *Store) ListConditions(ctx context.Context, includeResolved bool, limit int) ([]bus.OperatorCondition, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	now := s.now().UTC().UnixNano()
	if _, err := s.db.ExecContext(ctx, `
		UPDATE operator_conditions SET state = ?, snoozed_until_ns = NULL
		WHERE state = ? AND snoozed_until_ns IS NOT NULL AND snoozed_until_ns <= ?`,
		bus.ConditionActiveVisible, bus.ConditionActiveSnoozed, now); err != nil {
		return nil, fmt.Errorf("expire condition snoozes: %w", err)
	}
	query := `
		SELECT condition_kind, subject, generation, state, reason_code, summary, details_json,
		       first_seen_at_ns, last_seen_at_ns, resolved_at_ns, snoozed_until_ns,
		       acknowledged_at_ns, presentation_owner, presentation_lease_until_ns
		FROM operator_conditions`
	args := make([]interface{}, 0, 2)
	if !includeResolved {
		query += ` WHERE state <> ?`
		args = append(args, bus.ConditionResolved)
	}
	query += ` ORDER BY CASE state WHEN 'active_visible' THEN 0 WHEN 'active_snoozed' THEN 1 WHEN 'active_acknowledged' THEN 2 ELSE 3 END, last_seen_at_ns DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list operator conditions: %w", err)
	}
	defer rows.Close()
	conditions := make([]bus.OperatorCondition, 0)
	for rows.Next() {
		condition, err := scanCondition(rows)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
	}
	return conditions, rows.Err()
}

func (s *Store) AcknowledgeCondition(ctx context.Context, kind, subject string, generation int) (bus.OperatorCondition, error) {
	return s.changeConditionPresentation(ctx, kind, subject, generation, bus.ConditionActiveAcknowledged, time.Time{})
}

func (s *Store) SnoozeCondition(ctx context.Context, kind, subject string, generation int, until time.Time) (bus.OperatorCondition, error) {
	if !until.After(s.now().UTC()) {
		return bus.OperatorCondition{}, &bus.ValidationError{Field: "until", Problem: "must be in the future"}
	}
	return s.changeConditionPresentation(ctx, kind, subject, generation, bus.ConditionActiveSnoozed, until.UTC())
}

func (s *Store) changeConditionPresentation(ctx context.Context, kind, subject string, generation int, state bus.ConditionState, until time.Time) (bus.OperatorCondition, error) {
	kind, subject = strings.TrimSpace(kind), strings.TrimSpace(subject)
	if kind == "" || subject == "" || generation <= 0 {
		return bus.OperatorCondition{}, &bus.ValidationError{Field: "condition", Problem: "kind, subject, and positive generation are required"}
	}
	now := s.now().UTC()
	var snooze interface{}
	var acknowledged interface{}
	if state == bus.ConditionActiveSnoozed {
		snooze = until.UnixNano()
	} else {
		acknowledged = now.UnixNano()
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE operator_conditions SET state = ?, snoozed_until_ns = ?, acknowledged_at_ns = ?,
			presentation_owner = NULL, presentation_lease_until_ns = NULL
		WHERE condition_kind = ? AND subject = ? AND generation = ? AND state <> ?`,
		state, snooze, acknowledged, kind, subject, generation, bus.ConditionResolved)
	if err != nil {
		return bus.OperatorCondition{}, fmt.Errorf("update condition presentation: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return bus.OperatorCondition{}, err
	}
	if changed == 0 {
		return bus.OperatorCondition{}, bus.ErrNotFound
	}
	return s.getCondition(ctx, kind, subject)
}

func (s *Store) ClaimConditionPresentation(ctx context.Context, kind, subject string, generation int, presenter string, lease time.Duration) (bool, error) {
	presenter = strings.TrimSpace(presenter)
	if kind == "" || subject == "" || generation <= 0 || presenter == "" {
		return false, &bus.ValidationError{Field: "condition presentation", Problem: "kind, subject, generation, and presenter are required"}
	}
	if lease <= 0 || lease > 5*time.Minute {
		return false, &bus.ValidationError{Field: "presentation_lease", Problem: "must be between 0 and 5m"}
	}
	now := s.now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE operator_conditions SET presentation_owner = ?, presentation_lease_until_ns = ?
		WHERE condition_kind = ? AND subject = ? AND generation = ? AND state = ?
		  AND (presentation_owner IS NULL OR presentation_lease_until_ns <= ? OR presentation_owner = ?)`,
		presenter, now.Add(lease).UnixNano(), kind, subject, generation, bus.ConditionActiveVisible, now.UnixNano(), presenter)
	if err != nil {
		return false, fmt.Errorf("claim condition presentation: %w", err)
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

func (s *Store) ReconcileStaleUnreadConditions(ctx context.Context, staleAfter time.Duration) error {
	if staleAfter <= 0 {
		return &bus.ValidationError{Field: "stale_unread_after", Problem: "must be positive"}
	}
	now := s.now().UTC()
	type staleUnread struct {
		actor    string
		count    int
		oldestNS int64
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(a.adopting_actor, d.recipient_actor), COUNT(*), MIN(m.created_at_ns)
		FROM deliveries d
		JOIN messages m ON m.message_id = d.message_id
		LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
		WHERE m.delivery_request <> ?
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		  AND m.created_at_ns <= ?
		  AND (d.state = ? OR (d.state = ? AND d.lease_expires_at_ns <= ?))
		GROUP BY COALESCE(a.adopting_actor, d.recipient_actor)`,
		bus.DeliveryNonBlocking, now.UnixNano(), now.Add(-staleAfter).UnixNano(),
		bus.DeliveryQueued, bus.DeliveryClaimed, now.UnixNano())
	if err != nil {
		return fmt.Errorf("query stale unread work: %w", err)
	}
	stale := make([]staleUnread, 0)
	for rows.Next() {
		var item staleUnread
		if err := rows.Scan(&item.actor, &item.count, &item.oldestNS); err != nil {
			_ = rows.Close()
			return err
		}
		stale = append(stale, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	active := make(map[string]struct{}, len(stale))
	for _, item := range stale {
		active[item.actor] = struct{}{}
		details, _ := json.Marshal(map[string]interface{}{
			"actor": item.actor, "unclaimed": item.count,
			"oldest_unread_at":          time.Unix(0, item.oldestNS).UTC(),
			"oldest_unread_age_seconds": int64(now.Sub(time.Unix(0, item.oldestNS)) / time.Second),
		})
		if _, err := s.ObserveCondition(ctx, bus.ConditionObservation{
			Kind: "stale_unread", Subject: item.actor, ReasonCode: "wake_requested_unclaimed_threshold",
			Summary: fmt.Sprintf("%d wake-requested message(s) for %s remain unclaimed", item.count, item.actor),
			Details: details,
		}); err != nil {
			return err
		}
	}
	conditions, err := s.ListConditions(ctx, true, 500)
	if err != nil {
		return err
	}
	for _, condition := range conditions {
		if condition.Kind != "stale_unread" || condition.State == bus.ConditionResolved {
			continue
		}
		if _, exists := active[condition.Subject]; !exists {
			if err := s.ResolveCondition(ctx, condition.Kind, condition.Subject); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) getCondition(ctx context.Context, kind, subject string) (bus.OperatorCondition, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT condition_kind, subject, generation, state, reason_code, summary, details_json,
		       first_seen_at_ns, last_seen_at_ns, resolved_at_ns, snoozed_until_ns,
		       acknowledged_at_ns, presentation_owner, presentation_lease_until_ns
		FROM operator_conditions WHERE condition_kind = ? AND subject = ?`, kind, subject)
	condition, err := scanCondition(row)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.OperatorCondition{}, bus.ErrNotFound
	}
	return condition, err
}

func getConditionTx(ctx context.Context, tx *sql.Tx, kind, subject string) (bus.OperatorCondition, error) {
	return scanCondition(tx.QueryRowContext(ctx, `
		SELECT condition_kind, subject, generation, state, reason_code, summary, details_json,
		       first_seen_at_ns, last_seen_at_ns, resolved_at_ns, snoozed_until_ns,
		       acknowledged_at_ns, presentation_owner, presentation_lease_until_ns
		FROM operator_conditions WHERE condition_kind = ? AND subject = ?`, kind, subject))
}

type conditionScanner interface{ Scan(...interface{}) error }

func scanCondition(scanner conditionScanner) (bus.OperatorCondition, error) {
	var condition bus.OperatorCondition
	var details []byte
	var firstNS, lastNS int64
	var resolvedNS, snoozedNS, acknowledgedNS, presentationNS sql.NullInt64
	var presentationOwner sql.NullString
	if err := scanner.Scan(&condition.Kind, &condition.Subject, &condition.Generation, &condition.State,
		&condition.ReasonCode, &condition.Summary, &details, &firstNS, &lastNS, &resolvedNS,
		&snoozedNS, &acknowledgedNS, &presentationOwner, &presentationNS); err != nil {
		return bus.OperatorCondition{}, err
	}
	condition.Details = append(json.RawMessage(nil), details...)
	condition.FirstSeenAt = time.Unix(0, firstNS).UTC()
	condition.LastSeenAt = time.Unix(0, lastNS).UTC()
	condition.ResolvedAt = nullableConditionTime(resolvedNS)
	condition.SnoozedUntil = nullableConditionTime(snoozedNS)
	condition.AcknowledgedAt = nullableConditionTime(acknowledgedNS)
	condition.PresentationOwner = presentationOwner.String
	condition.PresentationLeaseUntil = nullableConditionTime(presentationNS)
	return condition, nil
}

func nullableConditionTime(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	result := time.Unix(0, value.Int64).UTC()
	return &result
}
