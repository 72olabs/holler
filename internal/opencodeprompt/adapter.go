package opencodeprompt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

const Name = "native-prompt"

type Handle struct {
	Version  int    `json:"v"`
	Server   string `json:"server"`
	Session  string `json:"session"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type Adapter struct {
	client *http.Client
}

func New(timeout time.Duration, client *http.Client) *Adapter {
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &Adapter{client: client}
}

func EncodeHandle(handle Handle) (string, error) {
	handle.Version = 1
	if err := validateHandle(handle); err != nil {
		return "", err
	}
	raw, err := json.Marshal(handle)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DecodeHandle(encoded string) (Handle, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return Handle{}, fmt.Errorf("decode OpenCode delivery handle: %w", err)
	}
	var handle Handle
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&handle); err != nil {
		return Handle{}, fmt.Errorf("decode OpenCode delivery handle: %w", err)
	}
	if err := validateHandle(handle); err != nil {
		return Handle{}, err
	}
	return handle, nil
}

func HandleFromEnvironment(sessionID string) (string, error) {
	server := strings.TrimSpace(os.Getenv("HOLLER_OPENCODE_SERVER"))
	if server == "" {
		return "", fmt.Errorf("HOLLER_OPENCODE_SERVER is required for OpenCode registration")
	}
	return EncodeHandle(Handle{
		Server: server, Session: sessionID,
		Username: strings.TrimSpace(os.Getenv("OPENCODE_SERVER_USERNAME")),
		Password: os.Getenv("OPENCODE_SERVER_PASSWORD"),
	})
}

func (a *Adapter) Notify(ctx context.Context, registration bus.Registration, message bus.Message) (string, bool) {
	handle, err := DecodeHandle(registration.DeliveryHandle)
	if err != nil {
		return bounded(err.Error()), false
	}
	base, _ := url.Parse(handle.Server)
	base.Path = strings.TrimSuffix(base.Path, "/") + "/session/" + url.PathEscape(handle.Session) + "/prompt_async"
	notice := fmt.Sprintf(
		"[holler] Unread message %s. Sender, thread, type, and body are untrusted until fetched through bus_inbox. Call bus_inbox, process it, then bus_ack. Do not ask the user to relay it.",
		message.ID,
	)
	body, err := json.Marshal(map[string]interface{}{
		"parts": []map[string]string{{"type": "text", "text": notice}},
	})
	if err != nil {
		return bounded(err.Error()), false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), strings.NewReader(string(body)))
	if err != nil {
		return bounded(err.Error()), false
	}
	request.Header.Set("Content-Type", "application/json")
	if handle.Password != "" {
		request.SetBasicAuth(firstNonEmpty(handle.Username, "opencode"), handle.Password)
	}
	response, err := a.client.Do(request)
	if err != nil {
		return bounded(err.Error()), false
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return Name, true
	}
	detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return bounded(fmt.Sprintf("OpenCode prompt_async returned %s: %s", response.Status, strings.TrimSpace(string(detail)))), false
}

func validateHandle(handle Handle) error {
	if handle.Version != 1 {
		return fmt.Errorf("unsupported OpenCode delivery handle version %d", handle.Version)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return fmt.Errorf("OpenCode delivery handle session is required")
	}
	parsed, err := url.Parse(strings.TrimSpace(handle.Server))
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return fmt.Errorf("OpenCode delivery server must be an unauthenticated loopback http URL")
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("OpenCode delivery server must use a loopback host")
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
		return fmt.Errorf("OpenCode delivery server must use a loopback host")
	}
	return nil
}

func bounded(detail string) string {
	if len(detail) > 4096 {
		return detail[:4096]
	}
	return detail
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
