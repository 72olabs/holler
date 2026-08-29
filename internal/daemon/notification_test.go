package daemon

import (
	"testing"

	"github.com/72olabs/holler/internal/bus"
)

func TestNotificationOutcomeAcceptedRegistrationWaitsForDurableClaim(t *testing.T) {
	attempts := []bus.NotificationAttempt{
		{RunID: "accepted-run", Result: "accepted", Detail: "hook-long-poll"},
		{RunID: "stale-run", Result: "retryable", Detail: "no active attention waiter"},
	}
	disposition, detail := notificationOutcome(attempts, nil)
	if disposition != bus.NotificationAccepted || detail != "accepted; awaiting recipient claim" {
		t.Fatalf("disposition=%v detail=%q", disposition, detail)
	}
}

func TestNotificationOutcomeRetriesWhenNoRegistrationAccepts(t *testing.T) {
	attempts := []bus.NotificationAttempt{
		{RunID: "unsupported-run", Result: "unsupported", Detail: "startup-only"},
		{RunID: "waiting-run", Result: "retryable", Detail: "no active attention waiter"},
	}
	disposition, detail := notificationOutcome(attempts, nil)
	if disposition != bus.NotificationRetry || detail != "no active attention waiter" {
		t.Fatalf("disposition=%v detail=%q", disposition, detail)
	}
}
