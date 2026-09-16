package gateway

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/store/sqlite"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed web/index.html web/app.js web/style.css
var assets embed.FS

type Store interface {
	api.ConversationStore
	RegisterHuman(context.Context, bus.ConversationPrincipal, string) error
	SupervisionPreflight(context.Context, bus.ConversationPrincipal, string) (sqlite.SupervisionPreview, error)
	SupervisionChange(context.Context, bus.ConversationPrincipal, sqlite.SupervisionPreview, string, string) (sqlite.SupervisionPreview, error)
}
type Config struct{ Address, CredentialPath, Human, Scope string }
type session struct {
	token               string
	run                 string
	lastUsed            time.Time
	expires, timeWindow time.Time
	requests            int
}
type Gateway struct {
	store         Store
	config        Config
	credential    credentials
	host          string
	mu            sync.Mutex
	sessions      []session
	failures      int
	failureWindow time.Time
	now           func() time.Time
}

// New does not listen or print credentials. Start must use the loopback listener
// below so the trusted Host cannot be derived from attacker-controlled headers.
func New(ctx context.Context, s Store, c Config) (*Gateway, error) {
	host, _, err := net.SplitHostPort(c.Address)
	if err != nil || host != "127.0.0.1" {
		return nil, errors.New("human gateway must bind explicitly to 127.0.0.1")
	}
	credential, err := loadCredentials(c.CredentialPath, c.Human, c.Scope)
	if err != nil {
		return nil, err
	}
	if err := s.RegisterHuman(ctx, bus.ConversationPrincipal{Actor: "operator", Run: "gateway-enrollment", Admin: true}, credential.Human); err != nil {
		return nil, fmt.Errorf("enroll human gateway identity (an existing legacy actor cannot become a human; choose a distinct --human-actor): %w", err)
	}
	return &Gateway{store: s, config: c, credential: credential, now: time.Now}, nil
}

func (g *Gateway) Listen() (net.Listener, error) {
	l, err := net.Listen("tcp4", g.config.Address)
	if err != nil {
		return nil, err
	}
	g.host = l.Addr().String()
	return l, nil
}
func (g *Gateway) Serve(ctx context.Context, l net.Listener) error {
	if l.Addr().String() != g.host {
		return errors.New("gateway listener mismatch")
	}
	server := &http.Server{Handler: g, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-done:
		}
	}()
	err := server.Serve(l)
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (g *Gateway) URL() string { return "http://" + g.host }

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func deny(w http.ResponseWriter, status int) {
	writeJSON(w, status, map[string]string{"error": "request not authorized"})
}
func decode(w http.ResponseWriter, r *http.Request, v interface{}) error {
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/json" {
		return errors.New("JSON required")
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2*bus.MaxBodyBytes))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(interface{})); err != io.EOF {
		return errors.New("one JSON value required")
	}
	return nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	if r.Host != g.host || r.URL.RawQuery != "" || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != g.URL()) {
		deny(w, 403)
		return
	}
	if r.Method == http.MethodGet {
		file := ""
		kind := ""
		switch r.URL.Path {
		case "/":
			file = "index.html"
			kind = "text/html; charset=utf-8"
		case "/app.js":
			file = "app.js"
			kind = "text/javascript; charset=utf-8"
		case "/style.css":
			file = "style.css"
			kind = "text/css; charset=utf-8"
		}
		if file == "" {
			http.NotFound(w, r)
			return
		}
		data, err := assets.ReadFile("web/" + file)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", kind)
		_, _ = w.Write(data)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		deny(w, 405)
		return
	}
	// Serializes credential validation/rotation and bounded per-session operations.
	// Revoked grants are independently checked transactionally by every store call.
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if r.URL.Path == "/login" {
		if now.Sub(g.failureWindow) >= time.Minute {
			g.failureWindow = now
			g.failures = 0
		}
		if g.failures >= 5 {
			w.Header().Set("Retry-After", "60")
			deny(w, 429)
			return
		}
		var input struct {
			Bearer string `json:"bearer"`
		}
		if decode(w, r, &input) != nil || subtle.ConstantTimeCompare([]byte(input.Bearer), []byte(g.credential.Bearer)) != 1 {
			g.failures++
			deny(w, 401)
			return
		}
		token, err := randomSecret()
		if err != nil {
			deny(w, 500)
			return
		}
		valid := g.sessions[:0]
		for _, s := range g.sessions {
			if s.expires.After(now) && now.Sub(s.lastUsed) < time.Hour {
				valid = append(valid, s)
			}
		}
		g.sessions = valid
		if len(g.sessions) >= 16 {
			deny(w, 429)
			return
		}
		run, err := randomSecret()
		if err != nil {
			deny(w, 500)
			return
		}
		g.sessions = append(g.sessions, session{token: token, run: "human-session-" + run, lastUsed: now, expires: now.Add(8 * time.Hour), timeWindow: now})
		writeJSON(w, 200, map[string]string{"session": token, "human": g.credential.Human, "scope": g.credential.Scope})
		return
	}
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		deny(w, 401)
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	index := -1
	for i, s := range g.sessions {
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) == 1 && s.expires.After(now) && now.Sub(s.lastUsed) < time.Hour {
			index = i
		}
	}
	if index < 0 {
		deny(w, 401)
		return
	}
	s := &g.sessions[index]
	s.lastUsed = now
	if now.Sub(s.timeWindow) >= time.Second {
		s.timeWindow = now
		s.requests = 0
	}
	s.requests++
	if s.requests > 10 {
		w.Header().Set("Retry-After", "1")
		deny(w, 429)
		return
	}
	p := bus.ConversationPrincipal{Actor: g.credential.Human, Run: s.run, Gateway: true, Admin: g.credential.Scope == "observe+admin"}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	switch r.URL.Path {
	case "/logout":
		var input struct{}
		if decode(w, r, &input) != nil {
			deny(w, 400)
			return
		}
		g.sessions = append(g.sessions[:index], g.sessions[index+1:]...)
		writeJSON(w, 200, map[string]bool{"ok": true})
	case "/rotate":
		var input struct{}
		if decode(w, r, &input) != nil {
			deny(w, 400)
			return
		}
		if !p.Admin {
			deny(w, 403)
			return
		}
		next := g.credential
		var err error
		next.Bearer, err = randomSecret()
		if err == nil {
			err = replaceCredentials(g.config.CredentialPath, next)
		}
		if err != nil {
			deny(w, 500)
			return
		}
		g.credential = next
		g.sessions = nil
		writeJSON(w, 200, map[string]string{"status": "rotated; log in with the new bearer from your credential file"})
	case "/rpc":
		var input struct {
			Operation string          `json:"operation"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := decode(w, r, &input); err != nil {
			writeJSON(w, 400, map[string]string{"error": "invalid JSON request"})
			return
		}
		if len(input.Arguments) == 0 {
			input.Arguments = json.RawMessage(`{}`)
		}
		var result interface{}
		var err error
		if input.Operation == "supervision.preflight" {
			var a struct {
				Agent string `json:"agent"`
			}
			err = strictArgs(input.Arguments, &a)
			if err == nil {
				result, err = g.store.SupervisionPreflight(ctx, p, a.Agent)
			}
		} else if input.Operation == "supervision.commit" {
			var a struct {
				Preview sqlite.SupervisionPreview `json:"preview"`
				Human   string                    `json:"human"`
				Key     string                    `json:"idempotency_key"`
			}
			err = strictArgs(input.Arguments, &a)
			if err == nil && a.Human != "" && a.Human != p.Actor {
				err = bus.ErrChannelDenied
			}
			if err == nil {
				result, err = g.store.SupervisionChange(ctx, p, a.Preview, a.Human, a.Key)
			}
		} else {
			result, err = api.InvokeConversation(ctx, g.store, p, input.Operation, input.Arguments)
		}
		if err != nil {
			status := 409
			code := api.ErrorDetails(err).Code
			switch {
			case errors.Is(err, bus.ErrChannelDenied):
				status = 404
			case errors.Is(err, bus.ErrInvalid):
				status = 400
			case errors.Is(err, bus.ErrChannelCapability):
				status = 400
			case errors.Is(err, bus.ErrShareAuthority):
				status = 403
			}
			if code == "internal" {
				status = 500
			}
			writeJSON(w, status, map[string]string{"error": code})
			return
		}
		writeJSON(w, 200, result)
	default:
		http.NotFound(w, r)
	}
}

func strictArgs(raw json.RawMessage, v interface{}) error {
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return &bus.ValidationError{Field: "arguments", Problem: "invalid shape"}
	}
	return nil
}
