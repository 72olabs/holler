package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func TestConversationMigrationFromActualV15Schema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v15.sqlite")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(schema); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO schema_migrations VALUES(15,1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO messages(message_id,schema_version,idempotency_key,project_id,channel_id,from_actor,from_run,message_type,delivery_request,body,created_at_ns) VALUES('old',1,'old','test','label','a','r','MESSAGE','non-blocking','{}',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO deliveries(message_id,recipient_actor,state) VALUES('old','b','queued')`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version int
	if err := s.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != 16 {
		t.Fatalf("migration version %d: %v", version, err)
	}
	items, err := s.CheckInbox(ctx, "b", 10)
	if err != nil || len(items) != 1 || items[0].MessageID != "old" {
		t.Fatalf("legacy lost: %+v %v", items, err)
	}
	var managed int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE conversation_id IS NOT NULL`).Scan(&managed); err != nil || managed != 0 {
		t.Fatalf("implicit history import: %d %v", managed, err)
	}
	backups, err := filepath.Glob(path + ".pre-v16.*.bak")
	if err != nil || len(backups) != 1 {
		t.Fatalf("missing verified backup: %v %v", backups, err)
	}
	for _, trigger := range []string{"messages_channel_immutable", "legacy_delivery_insert", "legacy_delivery_update", "managed_delivery_insert", "managed_delivery_update", "outbox_source_insert", "outbox_source_update", "legacy_event_insert", "legacy_event_update"} {
		var n int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?`, trigger).Scan(&n); err != nil || n != 1 {
			t.Fatalf("missing trigger %s: %d %v", trigger, n, err)
		}
	}
}

func channelFixture(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	for _, a := range []string{"a", "b", "c", "d"} {
		if err := s.ReserveActorName(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	return s, ctx
}
func agent(a string) bus.ConversationPrincipal {
	return bus.ConversationPrincipal{Actor: a, Run: a + "-run"}
}
func createDM(t *testing.T, s *Store, ctx context.Context, a, b, key string) bus.Channel {
	t.Helper()
	c, err := s.ChannelCreate(ctx, agent(a), bus.ChannelCreate{ProjectID: "test", Kind: "dm", Participants: []string{a, b}, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func postChannel(t *testing.T, s *Store, ctx context.Context, p bus.ConversationPrincipal, c bus.Channel, key string) bus.ChannelPostResult {
	t.Helper()
	r, err := s.ChannelPost(ctx, p, bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: key, Body: json.RawMessage(`{"text":"private channel body"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestChannelDMIdentityAndConcurrentReuse(t *testing.T) {
	s, ctx := channelFixture(t)
	var wg sync.WaitGroup
	results := make(chan string, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i))
			c, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "dm", Participants: []string{"b", "a"}, IdempotencyKey: key})
			if err != nil {
				errs <- err
			} else {
				results <- c.ID
			}
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	first := ""
	for id := range results {
		if first != "" && id != first {
			t.Fatal("duplicate DM")
		}
		first = id
	}
	other, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "other", Kind: "dm", Participants: []string{"a", "b"}, IdempotencyKey: "other"})
	if err != nil || other.ID == first {
		t.Fatalf("project boundary: %+v %v", other, err)
	}
	if _, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "dm", Participants: []string{"a", "b", "c"}, IdempotencyKey: "group"}); !errors.Is(err, bus.ErrImmutableAudience) {
		t.Fatalf("group DM gate: %v", err)
	}
}

func TestChannelPostHistoryReplyAndIdempotency(t *testing.T) {
	s, ctx := channelFixture(t)
	c := createDM(t, s, ctx, "a", "b", "dm")
	req := bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "m1", Body: json.RawMessage(`{"text":"secret"}`), Attention: []string{"b"}, Respondent: "b"}
	first, err := s.ChannelPost(ctx, agent("a"), req)
	if err != nil {
		t.Fatal(err)
	}
	dup, err := s.ChannelPost(ctx, agent("a"), req)
	if err != nil || !dup.Duplicate || dup.Message.ID != first.Message.ID {
		t.Fatalf("retry: %+v %v", dup, err)
	}
	req.Body = json.RawMessage(`{"text":"different"}`)
	if _, err := s.ChannelPost(ctx, agent("a"), req); !errors.Is(err, bus.ErrIdempotencyConflict) {
		t.Fatalf("idempotency: %v", err)
	}
	answer, err := s.ChannelPost(ctx, agent("b"), bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "answer", Body: json.RawMessage(`{"text":"answer"}`), InReplyTo: first.Message.ID, ResponseTo: first.Message.ResponseID, ExpectedResponseRevision: 1})
	if err != nil {
		t.Fatal(err)
	}
	if answer.Message.ThreadID != first.Message.ThreadID || answer.Message.InReplyTo != first.Message.ID {
		t.Fatal("reply structure")
	}
	page, err := s.ChannelHistory(ctx, agent("b"), c.ID, "", 0, 0, 50)
	if err != nil || len(page.Messages) != 2 {
		t.Fatalf("history: %+v %v", page, err)
	}
	if _, err := s.ChannelHistory(ctx, agent("c"), c.ID, "", 0, 0, 50); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("stranger history: %v", err)
	}
	other := createDM(t, s, ctx, "a", "c", "other")
	req = bus.ChannelPost{ChannelID: other.ID, ExpectedRevision: other.Revision, InReplyTo: first.Message.ID, IdempotencyKey: "bad-parent", Body: json.RawMessage(`{}`)}
	if _, err := s.ChannelPost(ctx, agent("a"), req); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("cross-channel parent: %v", err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM channel_responses WHERE state='answered' AND answer_id=?`, answer.Message.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("response not atomic: %d %v", count, err)
	}
}

func TestChannelObserverDoesNotConsumeDeliveries(t *testing.T) {
	s, ctx := channelFixture(t)
	// Fixture represents administrator-established registration and supervision;
	// these rows cannot be created by a legacy actor handshake.
	for _, stmt := range []string{
		`INSERT INTO actor_names VALUES('human:h',1)`,
		`INSERT INTO human_actors VALUES('human:h',1)`,
		`INSERT INTO supervision_links VALUES('a','human:h',1,1,'operator','admin',1)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	c := createDM(t, s, ctx, "a", "b", "dm")
	m := postChannel(t, s, ctx, agent("a"), c, "m")
	h := bus.ConversationPrincipal{Actor: "human:h", Run: "session", Gateway: true}
	page, err := s.ChannelHistory(ctx, h, c.ID, "", 0, 0, 50)
	if err != nil || len(page.Messages) != 1 {
		t.Fatalf("observer history: %+v %v", page, err)
	}
	if _, err := s.ChannelPost(ctx, h, bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "h-post", Body: json.RawMessage(`{}`)}); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("observer posted: %v", err)
	}
	if _, err := s.ChannelMessageGet(ctx, agent("human:h"), m.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("legacy human spoof: %v", err)
	}
	var state string
	var attempt int
	if err := s.db.QueryRow(`SELECT state,attempt FROM managed_deliveries WHERE message_id=? AND recipient_actor='b'`, m.Message.ID).Scan(&state, &attempt); err != nil || state != "queued" || attempt != 0 {
		t.Fatalf("observation consumed delivery: %s %d %v", state, attempt, err)
	}
	if _, err := s.db.Exec(`UPDATE channel_grants SET revoked_seq=100 WHERE channel_id=? AND actor='human:h'`, c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelMessageGet(ctx, h, m.Message.ID); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("revocation failed: %v", err)
	}
}

func TestManagedRowsInvisibleToLegacyAndCrossKindTriggers(t *testing.T) {
	s, ctx := channelFixture(t)
	c := createDM(t, s, ctx, "a", "b", "dm")
	m := postChannel(t, s, ctx, agent("a"), c, "managed")
	for _, a := range []string{"a", "b", "c", "operator"} {
		items, err := s.CheckInbox(ctx, a, 100)
		if err != nil || len(items) != 0 {
			t.Fatalf("legacy inbox %s: %+v %v", a, items, err)
		}
		if _, err := s.Claim(ctx, a, m.Message.ID, time.Minute); !errors.Is(err, bus.ErrNoMessage) {
			t.Fatalf("legacy claim %s: %v", a, err)
		}
		for _, stream := range []string{"durable", "operational"} {
			events, err := s.ListEvents(bus.WithCaller(ctx, bus.Caller{Actor: a}), "test", stream, 0, 100)
			if err != nil || len(events) != 0 {
				t.Fatalf("legacy events: %+v %v", events, err)
			}
		}
	}
	if _, err := s.ArchivePreflight(ctx, "a", 100); !errors.Is(err, bus.ErrChannelCapability) {
		t.Fatalf("managed archive guard: %v", err)
	}
	dir, err := s.Who(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(dir)
	if strings.Contains(string(raw), m.Message.ID) || strings.Contains(string(raw), "private channel body") {
		t.Fatalf("discovery leaked: %s", raw)
	}
	legacy, err := s.Send(ctx, bus.SendRequest{FromActor: "a", FromRun: "a-run", ToActors: []string{"b"}, ProjectID: "test", ChannelID: "direct", Type: "MESSAGE", Body: json.RawMessage(`{}`), IdempotencyKey: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`INSERT INTO deliveries(message_id,recipient_actor,state) VALUES('` + m.Message.ID + `','c','queued')`,
		`INSERT INTO managed_deliveries(message_id,recipient_actor) VALUES('` + legacy.Message.ID + `','c')`,
		`INSERT INTO notification_outbox(message_id,recipient_actor,state,available_at_ns,created_at_ns) VALUES('` + m.Message.ID + `','c','pending',0,0)`,
		`UPDATE messages SET conversation_id=NULL WHERE message_id='` + m.Message.ID + `'`,
		`INSERT INTO events(event_id,partition_id,stream,position,kind,message_id,created_at_ns) VALUES('bad','test','durable',99,'bad','` + m.Message.ID + `',0)`,
	} {
		if _, err := s.db.Exec(stmt); err == nil {
			t.Fatalf("cross-kind SQL succeeded: %s", stmt)
		}
	}
	if _, err := s.Send(ctx, bus.SendRequest{FromActor: "a", FromRun: "a-run", ToActors: []string{"b"}, ProjectID: "test", ChannelID: "direct", Type: "MESSAGE", Body: json.RawMessage(`{}`), IdempotencyKey: "managed"}); !errors.Is(err, bus.ErrIdempotencyConflict) {
		t.Fatalf("legacy duplicate exposed managed message: %v", err)
	}
}

func TestChannelReferencesProjectOnlyAuthorizedSource(t *testing.T) {
	s, ctx := channelFixture(t)
	ab := createDM(t, s, ctx, "a", "b", "ab")
	ac := createDM(t, s, ctx, "a", "c", "ac")
	source := postChannel(t, s, ctx, agent("a"), ab, "source")
	linked, err := s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: ac.ID, ExpectedRevision: ac.Revision, IdempotencyKey: "linked", Body: json.RawMessage(`{}`), References: []bus.ChannelReference{{SourceMessageID: source.Message.ID, Relation: "discusses"}}})
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.ChannelMessageGet(ctx, agent("c"), linked.Message.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.References) != 1 || view.References[0].Available || view.References[0].SourceMessageID != "" {
		t.Fatalf("source leaked: %+v", view)
	}
	if !linked.Message.References[0].Available {
		t.Fatal("authorized source hidden")
	}
	page, err := s.ChannelHistory(ctx, agent("b"), ab.ID, "", 0, 0, 50)
	if err != nil || len(page.Messages) != 1 {
		t.Fatalf("source changed: %+v %v", page, err)
	}
	before, _ := s.ChannelGet(ctx, agent("a"), ac.ID)
	_, err = s.db.Exec(`CREATE TRIGGER fail_reference BEFORE INSERT ON channel_references BEGIN SELECT RAISE(ABORT,'crash fixture'); END`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: ac.ID, ExpectedRevision: ac.Revision, IdempotencyKey: "rollback", Body: json.RawMessage(`{}`), References: []bus.ChannelReference{{SourceMessageID: source.Message.ID, Relation: "discusses"}}})
	if err == nil {
		t.Fatal("failure injection succeeded")
	}
	after, _ := s.ChannelGet(ctx, agent("a"), ac.ID)
	if after.LastSeq != before.LastSeq {
		t.Fatalf("partial sequence commit: before=%+v after=%+v", before, after)
	}
}
