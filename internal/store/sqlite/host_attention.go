package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/72olabs/holler/internal/bus"
)

// HostAttentionBinding returns the live host-injected registration for one
// exact Claude process generation. Callers must separately verify the
// connecting host's peer credentials.
func (s *Store) HostAttentionBinding(ctx context.Context, harnessPID int, harnessStart string) (bus.HostAttentionBinding, error) {
	if harnessPID <= 1 {
		return bus.HostAttentionBinding{}, &bus.ValidationError{Field: "claude_pid", Problem: "must identify an eligible process"}
	}
	if harnessStart == "" {
		return bus.HostAttentionBinding{}, &bus.ValidationError{Field: "claude_process", Problem: "start fingerprint is required"}
	}
	now := s.now().UTC().UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.HostAttentionBinding{}, fmt.Errorf("begin host attention lookup: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM host_attention_bindings
		WHERE NOT EXISTS (
			SELECT 1 FROM registrations r
			WHERE r.actor = host_attention_bindings.actor
			  AND r.run_id = host_attention_bindings.run_id
			  AND r.session_id = host_attention_bindings.session_id
			  AND r.harness = 'claude' AND r.attention_mode = 'host-injected'
			  AND r.ended_at_ns IS NULL AND r.attention_superseded_at_ns IS NULL
			  AND r.lease_expires_at_ns > ?
		)`, now); err != nil {
		return bus.HostAttentionBinding{}, fmt.Errorf("prune stale host attention bindings: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT harness_handle, harness_pid, harness_start, host_pid, host_start,
		       actor, run_id, session_id, admitted_at_ns IS NOT NULL
		FROM host_attention_bindings WHERE harness_pid = ? AND harness_start = ?`, harnessPID, harnessStart)
	if err != nil {
		return bus.HostAttentionBinding{}, fmt.Errorf("query host attention binding: %w", err)
	}
	defer rows.Close()
	var result bus.HostAttentionBinding
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return bus.HostAttentionBinding{}, err
		}
		return bus.HostAttentionBinding{}, bus.ErrNotFound
	}
	if err := scanHostAttentionBinding(rows, &result); err != nil {
		return bus.HostAttentionBinding{}, err
	}
	if err := rows.Err(); err != nil {
		return bus.HostAttentionBinding{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.HostAttentionBinding{}, fmt.Errorf("commit host attention lookup: %w", err)
	}
	return result, nil
}

func scanHostAttentionBinding(scanner interface{ Scan(...interface{}) error }, binding *bus.HostAttentionBinding) error {
	if err := scanner.Scan(
		&binding.HarnessHandle, &binding.Harness.PID, &binding.Harness.StartFingerprint,
		&binding.Host.PID, &binding.Host.StartFingerprint,
		&binding.Actor, &binding.RunID, &binding.SessionID, &binding.Admitted,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.ErrNotFound
		}
		return fmt.Errorf("scan host attention binding: %w", err)
	}
	return nil
}

func (s *Store) MarkHostAttentionAdmitted(ctx context.Context, binding bus.HostAttentionBinding) error {
	now := s.now().UTC().UnixNano()
	result, err := s.db.ExecContext(ctx, `
		UPDATE host_attention_bindings SET admitted_at_ns = ?, updated_at_ns = ?
		WHERE harness_pid = ? AND harness_start = ? AND actor = ? AND run_id = ? AND session_id = ?`,
		now, now, binding.Harness.PID, binding.Harness.StartFingerprint,
		binding.Actor, binding.RunID, binding.SessionID)
	if err != nil {
		return fmt.Errorf("mark host attention admitted: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect host attention admission: %w", err)
	}
	if updated != 1 {
		return bus.ErrNotFound
	}
	return nil
}
