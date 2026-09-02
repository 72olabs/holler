package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/72olabs/holler/internal/bus"
)

func (s *Store) DeliveryReceipts(ctx context.Context, message bus.Message) ([]bus.DeliveryReceipt, error) {
	requestedByActor, err := s.requestedRoutesByActor(ctx, message)
	if err != nil {
		return nil, err
	}
	receipts := make([]bus.DeliveryReceipt, 0, len(message.ToActors))
	for _, actor := range message.ToActors {
		route := bus.Route{Kind: bus.RouteActor, Value: actor}
		if requested, ok := requestedByActor[actor]; ok {
			route = requested
		}
		receipt := bus.DeliveryReceipt{
			MessageState: "committed", RequestedRoute: route, CanonicalRecipient: actor,
			RouteKind: route.Kind, DurableDelivery: "available", SenderAction: "none",
		}
		if message.DeliveryRequest == bus.DeliveryNonBlocking {
			receipt.ControlPresence = "unknown"
			receipt.AttentionCapability = "unknown"
			receipt.AttentionAttachment = "unknown"
			receipt.AttentionReason = "not_requested"
			receipts = append(receipts, receipt)
			continue
		}
		registrations, err := s.LiveRegistrations(ctx, actor)
		if err != nil {
			return nil, fmt.Errorf("inspect delivery availability for %s: %w", actor, err)
		}
		conditionReason, conditionSummary, err := s.activeAttentionCondition(ctx, actor)
		if err != nil {
			return nil, err
		}
		if len(registrations) == 0 {
			receipt.ControlPresence = "disconnected"
			receipt.AttentionCapability = "unknown"
			receipt.AttentionAttachment = "detached"
			receipt.AttentionReason = "session_not_registered"
			receipt.AttentionDetail = "message is durable; no live recipient session is registered"
			applyAttentionCondition(&receipt, conditionReason, conditionSummary)
			receipts = append(receipts, receipt)
			continue
		}
		receipt.ControlPresence = "connected"
		var enabled *bus.Registration
		for registrationIndex := range registrations {
			if registrations[registrationIndex].AttentionMode != "startup-only" {
				enabled = &registrations[registrationIndex]
				break
			}
		}
		if enabled == nil {
			receipt.AttentionCapability = "disabled_by_config"
			receipt.AttentionAttachment = "detached"
			receipt.AttentionReason = "startup_only_selected"
			receipt.AttentionDetail = "recipient selected startup-only attention and will hydrate this message on its next start"
			receipt.SenderAction = "inform_operator"
		} else {
			receipt.AttentionCapability = "enabled"
			receipt.AttentionAttachment = "attached"
			if enabled.Harness == "claude" && enabled.AttentionMode == "hook-long-poll" {
				receipt.AttentionAttachment = "reconnecting"
				receipt.AttentionReason = "monitor_reconnecting"
				receipt.AttentionDetail = "Claude attention is configured; the active monitor attachment is checked by the daemon"
			}
		}
		applyAttentionCondition(&receipt, conditionReason, conditionSummary)
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func (s *Store) requestedRoutesByActor(ctx context.Context, message bus.Message) (map[string]bus.Route, error) {
	result := make(map[string]bus.Route, len(message.ToActors))
	if len(message.RequestedRoutes) == 0 {
		for _, actor := range message.ToActors {
			result[actor] = bus.Route{Kind: bus.RouteActor, Value: actor}
		}
		return result, nil
	}
	aliasResolutions := make(map[string]string)
	needsAliasResolution := false
	for _, route := range message.RequestedRoutes {
		if route.Kind == bus.RouteAlias {
			needsAliasResolution = true
			break
		}
	}
	if needsAliasResolution {
		var payload []byte
		err := s.db.QueryRowContext(ctx, `
			SELECT payload FROM events
			WHERE message_id = ? AND stream = 'durable' AND kind = 'message.sent'
			ORDER BY position LIMIT 1`, message.ID).Scan(&payload)
		if err != nil {
			return nil, fmt.Errorf("read immutable route resolution for %s: %w", message.ID, err)
		}
		var provenance struct {
			AliasResolution map[string]string `json:"alias_resolution"`
		}
		if err := json.Unmarshal(payload, &provenance); err != nil {
			return nil, fmt.Errorf("decode immutable route resolution for %s: %w", message.ID, err)
		}
		aliasResolutions = provenance.AliasResolution
	}
	for _, route := range message.RequestedRoutes {
		actor := route.Value
		switch route.Kind {
		case bus.RouteAlias:
			actor = aliasResolutions[route.Value]
			if actor == "" {
				return nil, fmt.Errorf("immutable resolution for alias %s on message %s is unavailable", route.Value, message.ID)
			}
		case bus.RouteReply:
			if len(message.ToActors) != 1 {
				return nil, fmt.Errorf("reply message %s has %d canonical recipients", message.ID, len(message.ToActors))
			}
			actor = message.ToActors[0]
		}
		if _, exists := result[actor]; !exists {
			result[actor] = route
		}
	}
	return result, nil
}

func (s *Store) activeAttentionCondition(ctx context.Context, actor string) (string, string, error) {
	var reason, summary string
	err := s.db.QueryRowContext(ctx, `
		SELECT reason_code, summary FROM operator_conditions
		WHERE condition_kind = 'attention_unavailable' AND subject = ? AND state <> ?`,
		actor, bus.ConditionResolved).Scan(&reason, &summary)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("inspect attention condition for %s: %w", actor, err)
	}
	return reason, summary, nil
}

func applyAttentionCondition(receipt *bus.DeliveryReceipt, reason, summary string) {
	if reason == "" {
		return
	}
	switch reason {
	case "startup_only_selected":
		receipt.AttentionCapability = "disabled_by_config"
	case "harness_wake_unsupported":
		receipt.AttentionCapability = "version_blocked"
	default:
		receipt.AttentionCapability = "integration_missing"
	}
	receipt.AttentionAttachment = "detached"
	receipt.AttentionReason = reason
	receipt.AttentionDetail = summary
	receipt.SenderAction = "inform_operator"
}
