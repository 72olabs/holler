package bus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1

const MaxBodyBytes = 1 << 20

var (
	ErrInvalid             = errors.New("invalid request")
	ErrNotFound            = errors.New("not found")
	ErrNoMessage           = errors.New("no claimable message")
	ErrAttentionWaiterBusy = errors.New("attention waiter already active")
	ErrSessionEnded        = errors.New("session ended")
	ErrPresenceSuperseded  = errors.New("attention presence superseded")
	ErrRegistrationExpired = errors.New("registration expired")
	ErrIdempotencyConflict = errors.New("idempotency key reused for different message")
	ErrLeaseTokenMismatch  = errors.New("lease token mismatch")
	ErrLeaseExpired        = errors.New("lease expired")
	ErrDeliveryTerminal    = errors.New("delivery is already terminal")
)

type DeliveryRequest string

const (
	DeliveryNonBlocking DeliveryRequest = "non-blocking"
	DeliveryBlocking    DeliveryRequest = "blocking-requested"
	DeliveryWake        DeliveryRequest = "wake-requested"
)

type DeliveryState string

const (
	DeliveryQueued       DeliveryState = "queued"
	DeliveryClaimed      DeliveryState = "claimed"
	DeliveryAcked        DeliveryState = "acked"
	DeliveryDeadLettered DeliveryState = "dead-lettered"
)

type Message struct {
	ID              string          `json:"message_id"`
	SchemaVersion   int             `json:"schema_version"`
	IdempotencyKey  string          `json:"idempotency_key"`
	ProjectID       string          `json:"project_id"`
	ChannelID       string          `json:"channel_id"`
	ThreadID        string          `json:"thread_id,omitempty"`
	FromActor       string          `json:"from_actor"`
	FromRun         string          `json:"from_run"`
	FromRole        string          `json:"from_role,omitempty"`
	ToActors        []string        `json:"to_actors"`
	Type            string          `json:"type"`
	DeliveryRequest DeliveryRequest `json:"delivery_request"`
	InReplyTo       string          `json:"in_reply_to,omitempty"`
	Body            json.RawMessage `json:"body"`
	CreatedAt       time.Time       `json:"created_at"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
}

type SendRequest struct {
	IdempotencyKey  string          `json:"idempotency_key"`
	ProjectID       string          `json:"project_id"`
	ChannelID       string          `json:"channel_id"`
	ThreadID        string          `json:"thread_id,omitempty"`
	FromActor       string          `json:"from_actor,omitempty"`
	FromRun         string          `json:"from_run,omitempty"`
	FromRole        string          `json:"from_role,omitempty"`
	ToActors        []string        `json:"to_actors"`
	Type            string          `json:"type"`
	DeliveryRequest DeliveryRequest `json:"delivery_request"`
	InReplyTo       string          `json:"in_reply_to,omitempty"`
	Body            json.RawMessage `json:"body"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
}

type SendResult struct {
	Message           Message `json:"message"`
	Duplicate         bool    `json:"duplicate"`
	NotificationState string  `json:"notification_state,omitempty"`
}

type LeaseExtension struct {
	MessageID      string    `json:"message_id"`
	RecipientActor string    `json:"recipient_actor"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type NotificationJob struct {
	Message        Message `json:"message"`
	RecipientActor string  `json:"recipient_actor"`
	Attempt        int     `json:"attempt"`
}

type NotificationDisposition string

const (
	NotificationComplete NotificationDisposition = "complete"
	NotificationRetry    NotificationDisposition = "retry"
	NotificationAccepted NotificationDisposition = "accepted"
)

type InboxItem struct {
	MessageID       string          `json:"message_id"`
	ProjectID       string          `json:"project_id"`
	ChannelID       string          `json:"channel_id"`
	ThreadID        string          `json:"thread_id,omitempty"`
	FromActor       string          `json:"from_actor"`
	FromRole        string          `json:"from_role,omitempty"`
	Type            string          `json:"type"`
	DeliveryRequest DeliveryRequest `json:"delivery_request"`
	State           DeliveryState   `json:"state"`
	Attempt         int             `json:"attempt"`
	Available       bool            `json:"available"`
	CreatedAt       time.Time       `json:"created_at"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
}

type Claim struct {
	Message        Message   `json:"message"`
	RecipientActor string    `json:"recipient_actor"`
	Attempt        int       `json:"attempt"`
	LeaseToken     string    `json:"lease_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type Registration struct {
	Actor          string    `json:"actor"`
	RunID          string    `json:"run_id"`
	Harness        string    `json:"harness"`
	AttentionMode  string    `json:"attention_mode,omitempty"`
	SessionID      string    `json:"session_id"`
	DeliveryHandle string    `json:"delivery_handle"`
	ProjectID      string    `json:"project_id"`
	WorkingDir     string    `json:"working_directory,omitempty"`
	Epoch          int64     `json:"epoch"`
	UpdatedAt      time.Time `json:"updated_at"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type RegistrationRequest struct {
	Actor          string        `json:"actor,omitempty"`
	RunID          string        `json:"run_id,omitempty"`
	Harness        string        `json:"harness"`
	AttentionMode  string        `json:"attention_mode,omitempty"`
	SessionID      string        `json:"session_id"`
	DeliveryHandle string        `json:"delivery_handle"`
	ProjectID      string        `json:"project_id"`
	WorkingDir     string        `json:"working_directory,omitempty"`
	Lease          time.Duration `json:"lease"`
}

// ActorProfile is model-authored discovery metadata. It is descriptive only:
// no field in a profile changes delivery, authorization, or attention policy.
type ActorProfile struct {
	Actor        string    `json:"actor"`
	RoleText     string    `json:"role_text"`
	Accepts      []string  `json:"accepts,omitempty"`
	Revision     int64     `json:"revision"`
	UpdatedByRun string    `json:"updated_by_run"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type ActorProfileRequest struct {
	RoleText string   `json:"role_text"`
	Accepts  []string `json:"accepts,omitempty"`
}

type ActorProfileResult struct {
	Profile ActorProfile `json:"profile"`
	Updated bool         `json:"updated"`
}

type ActorSession struct {
	RunID          string     `json:"run_id"`
	Harness        string     `json:"harness"`
	AttentionMode  string     `json:"attention_mode,omitempty"`
	SessionID      string     `json:"session_id"`
	ProjectID      string     `json:"project_id"`
	WorkingDir     string     `json:"working_directory,omitempty"`
	State          string     `json:"state"`
	StartedAt      time.Time  `json:"started_at"`
	LastSeenAt     time.Time  `json:"last_seen_at"`
	LeaseExpiresAt time.Time  `json:"lease_expires_at"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
}

type ActorDirectoryEntry struct {
	Actor             string         `json:"actor"`
	State             string         `json:"state"`
	LastSeenAt        time.Time      `json:"last_seen_at"`
	Profile           *ActorProfile  `json:"profile,omitempty"`
	Sessions          []ActorSession `json:"sessions"`
	SessionsTruncated bool           `json:"sessions_truncated,omitempty"`
	UnclaimedMessages int            `json:"unclaimed_messages"`
}

type ActorDirectory struct {
	Actors        []ActorDirectoryEntry `json:"actors"`
	GeneratedAt   time.Time             `json:"generated_at"`
	Truncated     bool                  `json:"truncated"`
	MetadataTrust string                `json:"metadata_trust"`
}

type NotificationAttempt struct {
	Actor     string `json:"actor"`
	RunID     string `json:"run_id"`
	SessionID string `json:"session_id"`
	Harness   string `json:"harness"`
	Result    string `json:"result"`
	Detail    string `json:"detail,omitempty"`
}

// AttentionNotice is the reference-only payload delivered to a harness wake
// adapter. Message bodies remain behind the normal claim/lease boundary.
type AttentionNotice struct {
	MessageID       string          `json:"message_id"`
	ThreadID        string          `json:"thread_id,omitempty"`
	FromActor       string          `json:"from_actor"`
	Type            string          `json:"type"`
	DeliveryRequest DeliveryRequest `json:"delivery_request"`
}

type Event struct {
	ID          string          `json:"event_id"`
	PartitionID string          `json:"partition_id"`
	Stream      string          `json:"stream"`
	Position    int64           `json:"position"`
	Kind        string          `json:"kind"`
	MessageID   string          `json:"message_id,omitempty"`
	ActorID     string          `json:"actor_id,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type ValidationError struct {
	Field   string
	Problem string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Problem)
}

func (e *ValidationError) Unwrap() error { return ErrInvalid }

func NormalizeSendRequest(req SendRequest) (SendRequest, error) {
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.ProjectID = strings.TrimSpace(req.ProjectID)
	req.ChannelID = strings.TrimSpace(req.ChannelID)
	req.ThreadID = strings.TrimSpace(req.ThreadID)
	req.FromActor = strings.TrimSpace(req.FromActor)
	req.FromRun = strings.TrimSpace(req.FromRun)
	req.FromRole = strings.TrimSpace(req.FromRole)
	req.Type = strings.TrimSpace(req.Type)
	req.InReplyTo = strings.TrimSpace(req.InReplyTo)

	required := []struct {
		name  string
		value string
	}{
		{"idempotency_key", req.IdempotencyKey},
		{"project_id", req.ProjectID},
		{"channel_id", req.ChannelID},
		{"from_actor", req.FromActor},
		{"from_run", req.FromRun},
		{"type", req.Type},
	}
	for _, field := range required {
		if field.value == "" {
			return SendRequest{}, &ValidationError{Field: field.name, Problem: "is required"}
		}
	}

	if len(req.IdempotencyKey) > 256 {
		return SendRequest{}, &ValidationError{Field: "idempotency_key", Problem: "exceeds 256 bytes"}
	}
	if len(req.Body) == 0 {
		req.Body = json.RawMessage(`{}`)
	}
	if bytes.Equal(bytes.TrimSpace(req.Body), []byte("null")) {
		return SendRequest{}, &ValidationError{Field: "body", Problem: "must not be null"}
	}
	if len(req.Body) > MaxBodyBytes {
		return SendRequest{}, &ValidationError{Field: "body", Problem: "exceeds 1 MiB"}
	}
	if !json.Valid(req.Body) {
		return SendRequest{}, &ValidationError{Field: "body", Problem: "must be valid JSON"}
	}
	req.Body = append(json.RawMessage(nil), req.Body...)
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"project_id", req.ProjectID, 256}, {"channel_id", req.ChannelID, 128},
		{"thread_id", req.ThreadID, 256}, {"from_actor", req.FromActor, 128},
		{"from_run", req.FromRun, 256}, {"from_role", req.FromRole, 128},
		{"type", req.Type, 64}, {"in_reply_to", req.InReplyTo, 128},
	} {
		if err := ValidateTextIdentifier(field.name, field.value, field.max); err != nil {
			return SendRequest{}, err
		}
	}

	if req.DeliveryRequest == "" {
		req.DeliveryRequest = DeliveryNonBlocking
	}
	switch req.DeliveryRequest {
	case DeliveryNonBlocking, DeliveryBlocking, DeliveryWake:
	default:
		return SendRequest{}, &ValidationError{Field: "delivery_request", Problem: "is not supported"}
	}

	seen := make(map[string]struct{}, len(req.ToActors))
	recipients := make([]string, 0, len(req.ToActors))
	for _, actor := range req.ToActors {
		actor = strings.TrimSpace(actor)
		if actor == "" {
			continue
		}
		if err := ValidateTextIdentifier("to_actors", actor, 128); err != nil {
			return SendRequest{}, err
		}
		if _, exists := seen[actor]; exists {
			continue
		}
		seen[actor] = struct{}{}
		recipients = append(recipients, actor)
	}
	if len(recipients) == 0 {
		return SendRequest{}, &ValidationError{Field: "to_actors", Problem: "requires at least one actor"}
	}
	sort.Strings(recipients)
	req.ToActors = recipients

	if req.ExpiresAt != nil {
		expires := req.ExpiresAt.UTC()
		req.ExpiresAt = &expires
	}
	return req, nil
}

func ValidateTextIdentifier(name, value string, max int) error {
	if len(value) > max {
		return &ValidationError{Field: name, Problem: fmt.Sprintf("exceeds %d bytes", max)}
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return &ValidationError{Field: name, Problem: "contains control characters"}
		}
	}
	return nil
}

func EquivalentRequest(message Message, req SendRequest) bool {
	if message.IdempotencyKey != req.IdempotencyKey ||
		message.ProjectID != req.ProjectID ||
		message.ChannelID != req.ChannelID ||
		message.ThreadID != req.ThreadID ||
		message.FromActor != req.FromActor ||
		message.FromRun != req.FromRun ||
		message.FromRole != req.FromRole ||
		message.Type != req.Type ||
		message.DeliveryRequest != req.DeliveryRequest ||
		message.InReplyTo != req.InReplyTo ||
		!bytes.Equal(message.Body, req.Body) ||
		!equalStrings(message.ToActors, req.ToActors) {
		return false
	}
	if message.ExpiresAt == nil || req.ExpiresAt == nil {
		return message.ExpiresAt == nil && req.ExpiresAt == nil
	}
	return message.ExpiresAt.Equal(*req.ExpiresAt)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
