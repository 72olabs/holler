package bus

import (
	"context"
	"strings"
)

type Caller struct {
	Actor         string `json:"actor"`
	RunID         string `json:"run_id"`
	Client        string `json:"client"`
	BuildID       string `json:"build_id"`
	DaemonBuildID string `json:"daemon_build_id"`
}

type callerContextKey struct{}

func WithCaller(ctx context.Context, caller Caller) context.Context {
	caller.Actor = strings.TrimSpace(caller.Actor)
	caller.RunID = strings.TrimSpace(caller.RunID)
	caller.Client = strings.TrimSpace(caller.Client)
	caller.BuildID = strings.TrimSpace(caller.BuildID)
	caller.DaemonBuildID = strings.TrimSpace(caller.DaemonBuildID)
	return context.WithValue(ctx, callerContextKey{}, caller)
}

func CallerFromContext(ctx context.Context) Caller {
	caller, _ := ctx.Value(callerContextKey{}).(Caller)
	return caller
}

func EventProvenance(ctx context.Context, fallbackRun string) map[string]interface{} {
	caller := CallerFromContext(ctx)
	runID := caller.RunID
	if runID == "" {
		runID = strings.TrimSpace(fallbackRun)
	}
	payload := map[string]interface{}{"run_id": runID}
	if caller.Client != "" {
		payload["client"] = caller.Client
	}
	if caller.BuildID != "" {
		payload["client_build"] = caller.BuildID
	}
	if caller.DaemonBuildID != "" {
		payload["daemon_build"] = caller.DaemonBuildID
	}
	return payload
}
