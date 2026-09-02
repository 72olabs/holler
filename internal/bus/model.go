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
	ErrInvalid              = errors.New("invalid request")
	ErrNotFound             = errors.New("not found")
	ErrNoMessage            = errors.New("no claimable message")
	ErrAttentionWaiterBusy  = errors.New("attention waiter already active")
	ErrAttentionUnavailable = errors.New("attention waiting is unavailable")
	ErrSessionEnded         = errors.New("session ended")
	ErrPresenceSuperseded   = errors.New("attention presence superseded")
	ErrRegistrationExpired  = errors.New("registration expired")
	ErrIdempotencyConflict  = errors.New("idempotency key reused for different message")
	ErrLeaseTokenMismatch   = errors.New("lease token mismatch")
	ErrLeaseExpired         = errors.New("lease expired")
	ErrDeliveryTerminal     = errors.New("delivery is already terminal")
	ErrActorLive            = errors.New("actor already has a live presence")
	ErrBindingStale         = errors.New("actor binding is stale: this run was superseded and cannot reclaim the actor")
	ErrContinuityConflict   = errors.New("continuity handles resolve to different actors")
	ErrBindingReassigned    = errors.New("provisional actor binding was reassigned")
	ErrAdoptionConflict     = errors.New("actor inbox was already adopted by another actor")
	ErrAdoptionBusy         = errors.New("actor inbox has an active claim")
	ErrActorNotLive         = errors.New("adopting actor has no live presence")
	ErrRunNotLive           = errors.New("adopting run has no live presence")
	ErrActorAdopted         = errors.New("actor identity was permanently adopted")
	ErrAliasConflict        = errors.New("actor and alias namespaces conflict")
	ErrAliasNotFound        = errors.New("actor alias not found")
	ErrAliasTombstoned      = errors.New("actor alias was removed and is reserved")
	ErrAliasTargetUnknown   = errors.New("alias target is not a known actor")
	ErrDatabaseOwned        = errors.New("another hollerd already owns this database")
	ErrActorArchived        = errors.New("actor is archived")
)

type NameMode string

const (
	NameModeExact    NameMode = "exact"
	NameModeAllocate NameMode = "allocate"
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

// RouteKind distinguishes a mutable human-facing route from an immutable
// canonical actor identity. Reply routes are stamped by the store from the
// parent message and are never accepted as a caller-supplied destination.
type RouteKind string

const (
	RouteAlias RouteKind = "alias"
	RouteActor RouteKind = "actor"
	RouteReply RouteKind = "reply"
)

type Route struct {
	Kind  RouteKind `json:"kind"`
	Value string    `json:"value"`
}

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
	RequestedRoutes []Route         `json:"requested_routes,omitempty"`
	Type            string          `json:"type"`
	DeliveryRequest DeliveryRequest `json:"delivery_request"`
	InReplyTo       string          `json:"in_reply_to,omitempty"`
	Body            json.RawMessage `json:"body"`
	CreatedAt       time.Time       `json:"created_at"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	// RequestedToActors is retained by the durable store for idempotency
	// comparison. Public messages expose only the canonical actors stamped in
	// ToActors, never the aliases used by the sender.
	RequestedToActors []string `json:"-"`
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
	Destinations    []Route         `json:"destinations,omitempty"`
	Type            string          `json:"type"`
	DeliveryRequest DeliveryRequest `json:"delivery_request"`
	InReplyTo       string          `json:"in_reply_to,omitempty"`
	Body            json.RawMessage `json:"body"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	// RequestedToActors is populated internally before alias resolution.
	RequestedToActors []string `json:"-"`
	// RequestedRoutes is populated internally before route resolution and is
	// persisted as immutable send provenance.
	RequestedRoutes []Route `json:"-"`
}

type SendResult struct {
	Message           Message           `json:"message"`
	Duplicate         bool              `json:"duplicate"`
	NotificationState string            `json:"notification_state,omitempty"`
	DeliveryReceipts  []DeliveryReceipt `json:"delivery_receipts,omitempty"`
}

type DeliveryReceipt struct {
	MessageState        string    `json:"message"`
	RequestedRoute      Route     `json:"requested_route"`
	CanonicalRecipient  string    `json:"canonical_recipient"`
	RouteKind           RouteKind `json:"route_kind"`
	DurableDelivery     string    `json:"durable_delivery"`
	ControlPresence     string    `json:"control_presence"`
	AttentionCapability string    `json:"attention_capability"`
	AttentionAttachment string    `json:"attention_attachment"`
	AttentionReason     string    `json:"attention_reason,omitempty"`
	AttentionDetail     string    `json:"attention_detail,omitempty"`
	SenderAction        string    `json:"sender_action"`
}

type ConditionState string

const (
	ConditionActiveVisible      ConditionState = "active_visible"
	ConditionActiveSnoozed      ConditionState = "active_snoozed"
	ConditionActiveAcknowledged ConditionState = "active_acknowledged"
	ConditionResolved           ConditionState = "resolved"
)

type OperatorCondition struct {
	Kind                   string          `json:"kind"`
	Subject                string          `json:"subject"`
	Generation             int             `json:"generation"`
	State                  ConditionState  `json:"state"`
	ReasonCode             string          `json:"reason_code"`
	Summary                string          `json:"summary"`
	Details                json.RawMessage `json:"details,omitempty"`
	FirstSeenAt            time.Time       `json:"first_seen_at"`
	LastSeenAt             time.Time       `json:"last_seen_at"`
	ResolvedAt             *time.Time      `json:"resolved_at,omitempty"`
	SnoozedUntil           *time.Time      `json:"snoozed_until,omitempty"`
	AcknowledgedAt         *time.Time      `json:"acknowledged_at,omitempty"`
	PresentationOwner      string          `json:"presentation_owner,omitempty"`
	PresentationLeaseUntil *time.Time      `json:"presentation_lease_until,omitempty"`
}

type ActorArchiveMessage struct {
	MessageID        string    `json:"message_id"`
	FromActor        string    `json:"from_actor"`
	CreatedAt        time.Time `json:"created_at"`
	ThreadID         string    `json:"thread_id,omitempty"`
	Type             string    `json:"type"`
	BodyPreview      string    `json:"body_preview"`
	PreviewUntrusted bool      `json:"preview_untrusted"`
}

type ActorArchivePreflight struct {
	Actor              string                `json:"actor"`
	Archived           bool                  `json:"archived"`
	Aliases            []string              `json:"aliases"`
	Unread             []ActorArchiveMessage `json:"unread"`
	UnreadTruncated    bool                  `json:"unread_truncated"`
	ActiveClaims       int                   `json:"active_claims"`
	ControlPresence    int                   `json:"control_presence"`
	ContinuityBindings int                   `json:"continuity_bindings"`
	AutomaticEligible  bool                  `json:"automatic_eligible"`
	OperatorEligible   bool                  `json:"operator_eligible"`
	Blockers           []string              `json:"blockers"`
}

type ActorArchiveResult struct {
	Actor      string    `json:"actor"`
	Archived   bool      `json:"archived"`
	WithUnread bool      `json:"with_unread"`
	ChangedAt  time.Time `json:"changed_at"`
}

type ConditionObservation struct {
	Kind       string          `json:"kind"`
	Subject    string          `json:"subject"`
	ReasonCode string          `json:"reason_code"`
	Summary    string          `json:"summary"`
	Details    json.RawMessage `json:"details,omitempty"`
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
	MessageID              string          `json:"message_id"`
	ProjectID              string          `json:"project_id"`
	ChannelID              string          `json:"channel_id"`
	ThreadID               string          `json:"thread_id,omitempty"`
	FromActor              string          `json:"from_actor"`
	FromRole               string          `json:"from_role,omitempty"`
	Type                   string          `json:"type"`
	DeliveryRequest        DeliveryRequest `json:"delivery_request"`
	State                  DeliveryState   `json:"state"`
	Attempt                int             `json:"attempt"`
	Available              bool            `json:"available"`
	CreatedAt              time.Time       `json:"created_at"`
	ExpiresAt              *time.Time      `json:"expires_at,omitempty"`
	RecipientActor         string          `json:"recipient_actor"`
	OriginalRecipientActor string          `json:"original_recipient_actor,omitempty"`
}

type Claim struct {
	Message                Message   `json:"message"`
	RecipientActor         string    `json:"recipient_actor"`
	OriginalRecipientActor string    `json:"original_recipient_actor,omitempty"`
	Attempt                int       `json:"attempt"`
	LeaseToken             string    `json:"lease_token"`
	LeaseExpiresAt         time.Time `json:"lease_expires_at"`
}

type AdoptRequest struct {
	SourceActor    string `json:"source_actor"`
	AdoptingActor  string `json:"adopting_actor,omitempty"`
	AdoptingRun    string `json:"adopting_run,omitempty"`
	ProjectID      string `json:"project_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
}

type AdoptResult struct {
	SourceActor      string    `json:"source_actor"`
	AdoptingActor    string    `json:"adopting_actor"`
	AdoptingRun      string    `json:"adopting_run"`
	Transferred      int       `json:"transferred"`
	Deduplicated     int       `json:"deduplicated"`
	DuplicateRequest bool      `json:"duplicate_request"`
	AdoptedAt        time.Time `json:"adopted_at"`
	IdempotencyKey   string    `json:"-"`
}

type ActorAlias struct {
	Alias          string    `json:"alias"`
	Actor          string    `json:"actor"`
	Revision       int64     `json:"revision"`
	UpdatedByActor string    `json:"updated_by_actor"`
	UpdatedByRun   string    `json:"updated_by_run"`
	ProjectID      string    `json:"project_id"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type AliasSetRequest struct {
	Alias          string `json:"alias"`
	Actor          string `json:"actor"`
	UpdatedByActor string `json:"updated_by_actor,omitempty"`
	UpdatedByRun   string `json:"updated_by_run,omitempty"`
	ProjectID      string `json:"project_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type AliasClaimRequest struct {
	Alias          string `json:"alias"`
	Actor          string `json:"actor"`
	PolicyID       string `json:"policy_id"`
	Harness        string `json:"harness"`
	UpdatedByActor string `json:"updated_by_actor,omitempty"`
	UpdatedByRun   string `json:"updated_by_run,omitempty"`
	ProjectID      string `json:"project_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type AliasClaimResult struct {
	Alias            ActorAlias `json:"alias"`
	Claimed          bool       `json:"claimed"`
	PolicyID         string     `json:"policy_id"`
	DuplicateRequest bool       `json:"duplicate_request"`
}

type AliasRemoveRequest struct {
	Alias          string `json:"alias"`
	UpdatedByActor string `json:"updated_by_actor,omitempty"`
	UpdatedByRun   string `json:"updated_by_run,omitempty"`
	ProjectID      string `json:"project_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type AliasMutationResult struct {
	Alias            ActorAlias `json:"alias"`
	Removed          bool       `json:"removed"`
	DuplicateRequest bool       `json:"duplicate_request"`
}

type AliasPreflight struct {
	Alias                string                 `json:"alias"`
	State                string                 `json:"state"`
	CurrentTarget        string                 `json:"current_target,omitempty"`
	CurrentRevision      int64                  `json:"current_revision,omitempty"`
	ProposedTarget       string                 `json:"proposed_target,omitempty"`
	AliasesOnPredecessor []string               `json:"aliases_on_predecessor"`
	AliasesOnProposed    []string               `json:"aliases_on_proposed"`
	Predecessor          *ActorArchivePreflight `json:"predecessor,omitempty"`
	WholeActorAdoption   bool                   `json:"whole_actor_adoption"`
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
	ProjectID      string     `json:"project_id"`
	WorkingDir     string     `json:"working_directory,omitempty"`
	State          string     `json:"state"`
	StartedAt      time.Time  `json:"started_at"`
	LastSeenAt     time.Time  `json:"last_seen_at"`
	LeaseExpiresAt time.Time  `json:"lease_expires_at"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
}

type ActorDirectoryEntry struct {
	Actor                  string         `json:"actor"`
	State                  string         `json:"state"`
	LastSeenAt             time.Time      `json:"last_seen_at"`
	Profile                *ActorProfile  `json:"profile,omitempty"`
	Sessions               []ActorSession `json:"sessions"`
	SessionsTruncated      bool           `json:"sessions_truncated,omitempty"`
	UnclaimedMessages      int            `json:"unclaimed_messages"`
	OldestUnreadAt         *time.Time     `json:"oldest_unread_at,omitempty"`
	OldestUnreadAgeSeconds int64          `json:"oldest_unread_age_seconds"`
	ActiveClaims           int            `json:"active_claims"`
	EarliestLeaseExpiryAt  *time.Time     `json:"earliest_lease_expiry_at,omitempty"`
	StaleUnreadCondition   ConditionState `json:"stale_unread_condition,omitempty"`
}

type ActorDirectory struct {
	Actors        []ActorDirectoryEntry `json:"actors"`
	GeneratedAt   time.Time             `json:"generated_at"`
	Truncated     bool                  `json:"truncated"`
	MetadataTrust string                `json:"metadata_trust"`
}

type ActorBindRequest struct {
	RequestedActor    string   `json:"requested_actor"`
	RunID             string   `json:"run_id"`
	NameMode          NameMode `json:"name_mode"`
	ContinuityHandles []string `json:"continuity_handles,omitempty"`
	ProjectID         string   `json:"project_id,omitempty"`
	Takeover          bool     `json:"takeover,omitempty"`
}

type ActorBindResult struct {
	Actor                     string         `json:"actor"`
	AssignedRunID             string         `json:"assigned_run_id"`
	RequestedActor            string         `json:"requested_actor"`
	NameMode                  NameMode       `json:"name_mode"`
	Minted                    bool           `json:"minted"`
	Provisional               bool           `json:"provisional"`
	ContinuityReclaimed       bool           `json:"continuity_reclaimed"`
	AdoptedPredecessor        string         `json:"adopted_predecessor,omitempty"`
	PendingPredecessor        string         `json:"pending_predecessor,omitempty"`
	AcceptedContinuityHandles []string       `json:"-"`
	SupersededPresences       []Registration `json:"-"`
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

	if len(req.Destinations) > 0 && len(req.ToActors) > 0 {
		return SendRequest{}, &ValidationError{Field: "destinations", Problem: "cannot be combined with legacy to_actors"}
	}
	if len(req.Destinations) > 0 && req.InReplyTo != "" {
		return SendRequest{}, &ValidationError{Field: "destinations", Problem: "cannot be combined with in_reply_to"}
	}

	routeSeen := make(map[string]struct{}, len(req.Destinations))
	routes := make([]Route, 0, len(req.Destinations))
	for _, route := range req.Destinations {
		route.Value = strings.TrimSpace(route.Value)
		switch route.Kind {
		case RouteAlias, RouteActor:
		case RouteReply:
			return SendRequest{}, &ValidationError{Field: "destinations.kind", Problem: "reply routes must use in_reply_to"}
		default:
			return SendRequest{}, &ValidationError{Field: "destinations.kind", Problem: "must be alias or actor"}
		}
		if route.Value == "" {
			return SendRequest{}, &ValidationError{Field: "destinations.value", Problem: "is required"}
		}
		if err := ValidateTextIdentifier("destinations.value", route.Value, 128); err != nil {
			return SendRequest{}, err
		}
		key := string(route.Kind) + "\x00" + route.Value
		if _, exists := routeSeen[key]; exists {
			continue
		}
		routeSeen[key] = struct{}{}
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Kind == routes[j].Kind {
			return routes[i].Value < routes[j].Value
		}
		return routes[i].Kind < routes[j].Kind
	})
	req.Destinations = routes

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
	if len(recipients) == 0 && len(req.Destinations) == 0 && req.InReplyTo == "" {
		return SendRequest{}, &ValidationError{Field: "destinations", Problem: "requires an alias, actor, or in_reply_to"}
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
	messageRoutes := message.RequestedRoutes
	if len(messageRoutes) == 0 {
		messageRoutes = legacyRoutes(message.RequestedToActors)
		if len(messageRoutes) == 0 {
			messageRoutes = legacyRoutes(message.ToActors)
		}
	}
	requestRoutes := req.RequestedRoutes
	if len(requestRoutes) == 0 {
		requestRoutes = req.Destinations
	}
	if len(requestRoutes) == 0 {
		requestRoutes = legacyRoutes(req.RequestedToActors)
		if len(requestRoutes) == 0 {
			requestRoutes = legacyRoutes(req.ToActors)
		}
	}
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
		!equalRoutes(messageRoutes, requestRoutes) {
		return false
	}
	if message.ExpiresAt == nil || req.ExpiresAt == nil {
		return message.ExpiresAt == nil && req.ExpiresAt == nil
	}
	return message.ExpiresAt.Equal(*req.ExpiresAt)
}

func legacyRoutes(values []string) []Route {
	routes := make([]Route, 0, len(values))
	for _, value := range values {
		// An older row recorded only the raw compatibility string. An empty
		// kind preserves that uncertainty while still making retries compare by
		// the exact caller-supplied value.
		routes = append(routes, Route{Value: value})
	}
	return routes
}

func equalRoutes(a, b []Route) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Value != b[i].Value || (a[i].Kind != "" && b[i].Kind != "" && a[i].Kind != b[i].Kind) {
			return false
		}
	}
	return true
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
