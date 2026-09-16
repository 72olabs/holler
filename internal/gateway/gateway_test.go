package gateway

import (
	"context"
	"encoding/json"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/store/sqlite"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T, scope string) (*Gateway, *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	g, err := New(ctx, s, Config{Address: "127.0.0.1:0", Human: "human:owner", Scope: scope, CredentialPath: filepath.Join(t.TempDir(), "private", "login.json")})
	if err != nil {
		t.Fatal(err)
	}
	g.host = "127.0.0.1:43210"
	return g, s
}
func call(g *Gateway, path, body, token, origin string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", g.URL()+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}
func login(t *testing.T, g *Gateway) string {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"bearer": g.credential.Bearer})
	w := call(g, "/login", string(raw), "", g.URL())
	if w.Code != 200 {
		t.Fatalf("login %d: %s", w.Code, w.Body.String())
	}
	var out map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out["session"]
}

func TestGatewayAuthenticationOriginRotationAndRestart(t *testing.T) {
	g, s := fixture(t, "observe+admin")
	token := login(t, g)
	oldBearer := g.credential.Bearer
	for _, tc := range []struct {
		token, origin string
		status        int
	}{{"", "", 401}, {oldBearer, "", 401}, {token, "https://foreign.example", 403}, {token, g.URL(), 200}} {
		w := call(g, "/rpc", `{"operation":"channel.list","arguments":{}}`, tc.token, tc.origin)
		if w.Code != tc.status {
			t.Fatalf("auth status %d expected %d", w.Code, tc.status)
		}
	}
	r := httptest.NewRequest("POST", "http://attacker.example/rpc", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("host rebinding accepted")
	}
	if w := call(g, "/rpc?token=secret", `{}`, token, ""); w.Code != 403 {
		t.Fatal("query credential accepted")
	}
	if w := call(g, "/rotate", `{}`, token, g.URL()); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if oldBearer == g.credential.Bearer {
		t.Fatal("bearer unchanged")
	}
	if w := call(g, "/rpc", `{}`, token, ""); w.Code != 401 {
		t.Fatal("session survives rotation")
	}
	token = login(t, g)
	restarted, err := New(context.Background(), s, g.config)
	if err != nil {
		t.Fatal(err)
	}
	restarted.host = g.host
	if restarted.credential.Bearer != g.credential.Bearer {
		t.Fatal("restart changed stable enrollment")
	}
	if w := call(restarted, "/rpc", `{}`, token, ""); w.Code != 401 {
		t.Fatal("restart retained session")
	}
	info, err := os.Stat(g.config.CredentialPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("credential mode: %v %v", info, err)
	}
	r = httptest.NewRequest("GET", g.URL()+"/", nil)
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), g.credential.Bearer) {
		t.Fatal("unsafe asset response")
	}
}

func TestGatewayThrottleBoundsAndScope(t *testing.T) {
	g, _ := fixture(t, "observe")
	for i := 0; i < 5; i++ {
		if w := call(g, "/login", `{"bearer":"wrong"}`, "", ""); w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
	if w := call(g, "/login", `{"bearer":"wrong"}`, "", ""); w.Code != 429 {
		t.Fatal("login not throttled")
	}
	g.failureWindow = g.now().Add(-2 * time.Minute)
	token := login(t, g)
	if w := call(g, "/rotate", `{}`, token, ""); w.Code != 403 {
		t.Fatal("observer rotated admin credentials")
	}
	if w := call(g, "/rpc", `{"operation":"supervision.preflight","arguments":{"agent":"a"}}`, token, ""); w.Code != 404 {
		t.Fatal("observer acquired admin authority")
	}
	if w := call(g, "/rpc", `{"operation":"channel.list","arguments":{},"gateway":true}`, token, ""); w.Code != 400 {
		t.Fatal("unknown principal accepted")
	}
	if w := call(g, "/rpc", strings.Repeat("x", 2*bus.MaxBodyBytes+1), token, ""); w.Code != 400 {
		t.Fatal("unbounded request")
	}
	for i := 0; i < 10; i++ {
		call(g, "/rpc", `{"operation":"channel.list","arguments":{}}`, token, "")
	}
	if w := call(g, "/rpc", `{}`, token, ""); w.Code != 429 {
		t.Fatal("poll not bounded")
	}
}

func TestGatewayObserveThenParticipateAsHuman(t *testing.T) {
	g, s := fixture(t, "observe+admin")
	ctx := context.Background()
	for _, a := range []string{"a", "b"} {
		if err := s.ReserveActorName(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	h := bus.ConversationPrincipal{Actor: g.credential.Human, Run: "gateway", Gateway: true, Admin: true}
	a := bus.ConversationPrincipal{Actor: "a", Run: "r"}
	preview, err := s.SupervisionPreflight(ctx, h, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupervisionChange(ctx, h, preview, h.Actor, "link"); err != nil {
		t.Fatal(err)
	}
	channel, err := s.ChannelCreate(ctx, a, bus.ChannelCreate{ProjectID: "test", Kind: "dm", Participants: []string{"a", "b"}, IdempotencyKey: "dm"})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := s.ChannelPost(ctx, a, bus.ChannelPost{ChannelID: channel.ID, ExpectedRevision: channel.Revision, IdempotencyKey: "question", Body: json.RawMessage(`{"question":"What next?","risks":"May delay the work"}`)})
	if err != nil {
		t.Fatal(err)
	}
	token := login(t, g)
	raw, _ := json.Marshal(map[string]interface{}{"operation": "channel.history", "arguments": map[string]string{"channel_id": channel.ID}})
	w := call(g, "/rpc", string(raw), token, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "What next?") {
		t.Fatal(w.Body.String())
	}
	raw, _ = json.Marshal(map[string]interface{}{"operation": "channel.post", "arguments": bus.ChannelPost{ChannelID: channel.ID, ExpectedRevision: channel.Revision, IdempotencyKey: "forge", Body: json.RawMessage(`"reply"`)}})
	if w := call(g, "/rpc", string(raw), token, ""); w.Code != 404 {
		t.Fatal("observer posted directly")
	}
	intent := sqlite.ContinuationIntent{SourceMessageID: msg.Message.ID, Create: &bus.ChannelCreate{ProjectID: "test", Kind: "dm", Participants: []string{h.Actor, "a"}}, Body: json.RawMessage(`"What are the tradeoffs?"`)}
	raw, _ = json.Marshal(map[string]interface{}{"operation": "channel.continuation.preflight", "arguments": intent})
	w = call(g, "/rpc", string(raw), token, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var continuation sqlite.ContinuationPreview
	if err := json.Unmarshal(w.Body.Bytes(), &continuation); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(map[string]interface{}{"operation": "channel.continuation.commit", "arguments": map[string]string{"preflight_token": continuation.Token, "idempotency_key": "participate"}})
	w = call(g, "/rpc", string(raw), token, "")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var posted bus.ChannelPostResult
	if err := json.Unmarshal(w.Body.Bytes(), &posted); err != nil {
		t.Fatal(err)
	}
	if posted.Message.FromActor != h.Actor || posted.Message.ChannelID == channel.ID {
		t.Fatalf("human impersonation or source move: %+v", posted)
	}
	legacy, err := s.CheckInbox(ctx, "a", 10)
	if err != nil || len(legacy) != 0 {
		t.Fatal("managed human message in legacy inbox")
	}
}

func TestGatewayRejectsPublicBindAndCredentialCollision(t *testing.T) {
	g, s := fixture(t, "observe")
	c := g.config
	c.Address = "0.0.0.0:8080"
	if _, err := New(context.Background(), s, c); err == nil {
		t.Fatal("public bind")
	}
	c = g.config
	c.Scope = "observe+admin"
	if _, err := New(context.Background(), s, c); err == nil {
		t.Fatal("silent scope escalation")
	}
	if err := os.Chmod(g.config.CredentialPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), s, g.config); err == nil {
		t.Fatal("public credentials accepted")
	}
}

func TestGatewayRequestShapeSessionExpiryLogoutAndForeignHuman(t *testing.T) {
	g, _ := fixture(t, "observe+admin")
	token := login(t, g)
	for _, body := range []string{`{} {}`, `{"operation":"channel.list","arguments":{"gateway":true}}`} {
		if w := call(g, "/rpc", body, token, ""); w.Code != 400 {
			t.Fatalf("malformed %d: %s", w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest("POST", g.URL()+"/rpc", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("non-JSON accepted")
	}
	if w := call(g, "/rpc", `{"operation":"supervision.commit","arguments":{"preview":{"agent":"a"},"human":"human:someone-else","idempotency_key":"delegate"}}`, token, ""); w.Code != 404 {
		t.Fatalf("foreign supervisor: %s", w.Body.String())
	}
	firstRun := g.sessions[0].run
	secondToken := login(t, g)
	if g.sessions[1].run == firstRun || strings.Contains(g.sessions[1].run, secondToken) {
		t.Fatal("session audit identity reused or contains secret")
	}
	if w := call(g, "/logout", `{}`, token, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := call(g, "/rpc", `{}`, token, ""); w.Code != 401 {
		t.Fatal("logged out session accepted")
	}
	g.sessions[0].lastUsed = g.now().Add(-2 * time.Hour)
	if w := call(g, "/rpc", `{}`, secondToken, ""); w.Code != 401 {
		t.Fatal("idle session accepted")
	}
	token = login(t, g)
	g.sessions[0].expires = g.now().Add(-time.Second)
	if w := call(g, "/rpc", `{}`, token, ""); w.Code != 401 {
		t.Fatal("expired session accepted")
	}
}
