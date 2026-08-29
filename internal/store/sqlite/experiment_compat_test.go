package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

// TestAmbientExperimentQuestionAnswerFlow replays yesterday's transport edge
// without model sessions: the implementer asks before the owner starts, the
// owner hydrates from durable state, answers in-thread, and the implementer
// acknowledges the response. Harness registration and live wake are certified
// in later phases because they are connector concerns, not store behavior.
func TestAmbientExperimentQuestionAnswerFlow(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()

	question, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey:  "experiment-question",
		ProjectID:       "ambient-experiment",
		ChannelID:       "direct",
		ThreadID:        "thread-retry-policy",
		FromActor:       "implementer",
		FromRun:         "implementer-run-1",
		FromRole:        "implementer",
		ToActors:        []string{"owner"},
		Type:            "QUESTION",
		DeliveryRequest: bus.DeliveryWake,
		Body:            json.RawMessage(`{"text":"Which statuses, attempts, and delays are binding?"}`),
	})
	if err != nil {
		t.Fatalf("send offline question: %v", err)
	}

	// This check represents SessionStart hydration. It must observe metadata
	// without consuming or claiming the question.
	hydrated, err := db.CheckInbox(ctx, "owner", 10)
	if err != nil {
		t.Fatalf("owner startup hydration: %v", err)
	}
	if len(hydrated) != 1 || hydrated[0].MessageID != question.Message.ID || !hydrated[0].Available {
		t.Fatalf("hydrated inbox = %+v", hydrated)
	}
	ownerClaim, err := db.Claim(ctx, "owner", question.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("owner claim: %v", err)
	}
	if err := db.Ack(ctx, "owner", question.Message.ID, ownerClaim.LeaseToken); err != nil {
		t.Fatalf("owner ack question: %v", err)
	}

	answer, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey:  "experiment-answer",
		ProjectID:       "ambient-experiment",
		ChannelID:       "direct",
		FromActor:       "owner",
		FromRun:         "owner-run-1",
		FromRole:        "owner",
		ToActors:        []string{"implementer"},
		Type:            "ANSWER",
		DeliveryRequest: bus.DeliveryWake,
		InReplyTo:       question.Message.ID,
		Body: json.RawMessage(
			`{"text":"Retry 429/503 for attempts 0 and 1; cap Retry-After at 30 seconds, otherwise wait 1 then 2 seconds."}`,
		),
	})
	if err != nil {
		t.Fatalf("send owner answer: %v", err)
	}
	if answer.Message.ThreadID != question.Message.ThreadID || answer.Message.InReplyTo != question.Message.ID {
		t.Fatalf("answer threading = %+v, question = %+v", answer.Message, question.Message)
	}

	implementerClaim, err := db.Claim(ctx, "implementer", answer.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("implementer claim answer: %v", err)
	}
	if err := db.Ack(ctx, "implementer", answer.Message.ID, implementerClaim.LeaseToken); err != nil {
		t.Fatalf("implementer ack answer: %v", err)
	}
	if remaining, err := db.CheckInbox(ctx, "implementer", 10); err != nil || len(remaining) != 0 {
		t.Fatalf("implementer remaining inbox = %+v, err=%v", remaining, err)
	}

	durable, err := db.ListEvents(ctx, "ambient-experiment", "durable", 0, 10)
	if err != nil {
		t.Fatalf("list durable evidence: %v", err)
	}
	if len(durable) != 2 || durable[0].Kind != "message.sent" || durable[1].Kind != "message.sent" {
		t.Fatalf("durable evidence = %+v", durable)
	}
	operational, err := db.ListEvents(ctx, "ambient-experiment", "operational", 0, 20)
	if err != nil {
		t.Fatalf("list operational evidence: %v", err)
	}
	if len(operational) != 6 {
		t.Fatalf("operational event count = %d, want 6: %+v", len(operational), operational)
	}
}
