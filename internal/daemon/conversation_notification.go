package daemon

import (
	"context"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/connector"
	"time"
)

type managedNotificationQueue interface {
	ClaimManagedNotification(context.Context) (bus.NotificationJob, error)
	FinishManagedNotification(context.Context, bus.NotificationJob, bus.NotificationDisposition, string) error
	RearmStaleManagedNotifications(context.Context, time.Duration) error
}

// Managed results never enter legacy global conditions or notification events.
func runManagedNotificationWorker(ctx context.Context, q managedNotificationQueue, n *connector.Runtime, staleAfter time.Duration) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	rearm := time.NewTicker(30 * time.Second)
	defer rearm.Stop()
	_ = q.RearmStaleManagedNotifications(ctx, staleAfter)
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := q.ClaimManagedNotification(ctx)
		if err == nil {
			attempts, notifyErr := n.Notify(ctx, job.RecipientActor, job.Message)
			disposition, detail := notificationOutcome(attempts, notifyErr)
			_ = q.FinishManagedNotification(ctx, job, disposition, detail)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-rearm.C:
			_ = q.RearmStaleManagedNotifications(ctx, staleAfter)
		}
	}
}
