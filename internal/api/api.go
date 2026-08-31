package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/fdliveness"
)

const (
	ProtocolVersion           = 1
	MaxFrameBytes             = 2 << 20
	MaxAttentionWait          = 25 * time.Second
	ActorAllocationCapability = "actor-allocation-v1"
)

type Identity struct {
	Actor             string
	RunID             string
	Client            string
	Build             buildinfo.Info
	NameMode          bus.NameMode
	ContinuityHandles []string
	ProjectID         string
	Takeover          bool
	Provisional       bool
}

type Request struct {
	ID   uint64          `json:"id"`
	Op   string          `json:"op"`
	Args json.RawMessage `json:"args,omitempty"`
}

type Response struct {
	ID     uint64          `json:"id"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type DaemonInfo struct {
	Protocol     int            `json:"protocol"`
	Actor        string         `json:"actor"`
	PID          int            `json:"pid"`
	Build        buildinfo.Info `json:"build"`
	Capabilities []string       `json:"capabilities,omitempty"`
}

type rpcResponseError struct{ err error }

func (e *rpcResponseError) Error() string { return e.err.Error() }
func (e *rpcResponseError) Unwrap() error { return e.err }

type Store interface {
	Send(context.Context, bus.SendRequest) (bus.SendResult, error)
	CheckInbox(context.Context, string, int) ([]bus.InboxItem, error)
	Claim(context.Context, string, string, time.Duration) (bus.Claim, error)
	Ack(context.Context, string, string, string) error
	Extend(context.Context, string, string, string, time.Duration) (bus.LeaseExtension, error)
	Nack(context.Context, string, string, string, string, bool) error
	ListEvents(context.Context, string, string, int64, int) ([]bus.Event, error)
	RegisterSession(context.Context, bus.RegistrationRequest) (bus.Registration, error)
	AttachMonitor(context.Context, string, string, string, string, string, time.Duration) (bus.Registration, error)
	RearmAcceptedNotifications(context.Context, string) error
	LiveRegistrations(context.Context, string) ([]bus.Registration, error)
	RecordHydration(context.Context, string, string, string, string, string, int) error
	ExpireRegistration(context.Context, string, string, string, string) error
	HeartbeatRegistrations(context.Context, string, string, time.Duration) (int, error)
	SetActorProfile(context.Context, string, string, string, bus.ActorProfileRequest) (bus.ActorProfileResult, error)
	Who(context.Context, int) (bus.ActorDirectory, error)
	AdoptActor(context.Context, bus.AdoptRequest) (bus.AdoptResult, error)
	BindActor(context.Context, bus.ActorBindRequest) (bus.ActorBindResult, error)
	FinalizeActorAllocation(context.Context, string, string, string, []string) error
	CurrentActorForContinuity(context.Context, []string) (string, error)
	ReleaseProvisionalActor(context.Context, string) error
}

type AttentionBroker interface {
	Wait(context.Context, string, string, string, string) (bus.AttentionNotice, error)
	Attach(string, string, string, func() error) error
	Cancel(string, string, string, error)
}

type Server struct {
	store     Store
	build     buildinfo.Info
	attention AttentionBroker
}

type ServerOption func(*Server)

func NewServer(store Store, options ...ServerOption) *Server {
	server := &Server{store: store, build: buildinfo.Current()}
	for _, option := range options {
		option(server)
	}
	return server
}

func WithAttentionBroker(broker AttentionBroker) ServerOption {
	return func(server *Server) { server.attention = broker }
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	var activeMu sync.Mutex
	active := make(map[net.Conn]struct{})
	go func() {
		<-ctx.Done()
		_ = listener.Close()
		activeMu.Lock()
		defer activeMu.Unlock()
		for connection := range active {
			_ = connection.Close()
		}
	}()
	var connections sync.WaitGroup
	defer connections.Wait()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept API connection: %w", err)
		}
		activeMu.Lock()
		if ctx.Err() != nil {
			activeMu.Unlock()
			_ = connection.Close()
			continue
		}
		active[connection] = struct{}{}
		activeMu.Unlock()
		connections.Add(1)
		go func() {
			defer connections.Done()
			defer func() {
				activeMu.Lock()
				delete(active, connection)
				activeMu.Unlock()
			}()
			defer connection.Close()
			s.serveConnection(ctx, connection)
		}()
	}
}

func (s *Server) serveConnection(ctx context.Context, connection net.Conn) {
	connectionCtx, cancelConnection := context.WithCancel(ctx)
	defer cancelConnection()
	if descriptor, ok := duplicateSocketDescriptor(connection); ok {
		closed := fdliveness.Watch(connectionCtx, descriptor)
		go func() {
			select {
			case <-closed:
				cancelConnection()
			case <-connectionCtx.Done():
			}
		}()
	}
	reader := bufio.NewReader(connection)
	request, err := readRequest(reader)
	if err != nil {
		return
	}
	if request.Op != "hello" {
		_ = writeResponse(connection, failure(request.ID, "unauthenticated", "first operation must be hello", false))
		return
	}
	var hello struct {
		Protocol          int            `json:"protocol"`
		Client            string         `json:"client"`
		Actor             string         `json:"actor"`
		RunID             string         `json:"run_id"`
		Build             buildinfo.Info `json:"build"`
		Capabilities      []string       `json:"capabilities"`
		NameMode          bus.NameMode   `json:"name_mode"`
		ContinuityHandles []string       `json:"continuity_handles"`
		ProjectID         string         `json:"project_id"`
		Takeover          bool           `json:"takeover"`
	}
	if err := decodeStrict(request.Args, &hello); err != nil {
		_ = writeResponse(connection, failure(request.ID, "bad_request", err.Error(), false))
		return
	}
	hello.Actor = strings.TrimSpace(hello.Actor)
	hello.RunID = strings.TrimSpace(hello.RunID)
	hello.ProjectID = strings.TrimSpace(hello.ProjectID)
	for index := range hello.ContinuityHandles {
		hello.ContinuityHandles[index] = strings.TrimSpace(hello.ContinuityHandles[index])
	}
	if hello.Protocol != ProtocolVersion {
		_ = writeResponse(connection, failure(request.ID, "protocol_mismatch", fmt.Sprintf("protocol %d is not supported", hello.Protocol), false))
		return
	}
	if hello.Actor == "" || hello.RunID == "" {
		_ = writeResponse(connection, failure(request.ID, "unauthenticated", "actor and run_id are required", false))
		return
	}
	if err := bus.ValidateTextIdentifier("actor", hello.Actor, 128); err != nil {
		_ = writeResponse(connection, failure(request.ID, "bad_request", err.Error(), false))
		return
	}
	if err := bus.ValidateTextIdentifier("run_id", hello.RunID, 256); err != nil {
		_ = writeResponse(connection, failure(request.ID, "bad_request", err.Error(), false))
		return
	}
	assignedActor := hello.Actor
	var binding bus.ActorBindResult
	if hello.NameMode != "" || len(hello.ContinuityHandles) > 0 || hello.Takeover {
		if hello.ProjectID == "" {
			hello.ProjectID = "default"
		}
		if !containsString(hello.Capabilities, ActorAllocationCapability) {
			_ = writeResponse(connection, failure(request.ID, "capability_required", "actor allocation requires capability "+ActorAllocationCapability, false))
			return
		}
		bindCtx := bus.WithCaller(connectionCtx, bus.Caller{
			Actor: hello.Actor, RunID: hello.RunID, Client: hello.Client,
			BuildID: hello.Build.ID(), DaemonBuildID: s.build.ID(),
		})
		binding, err = s.store.BindActor(bindCtx, bus.ActorBindRequest{
			RequestedActor: hello.Actor, RunID: hello.RunID, NameMode: hello.NameMode,
			ContinuityHandles: hello.ContinuityHandles, ProjectID: hello.ProjectID, Takeover: hello.Takeover,
		})
		if err != nil {
			_ = writeResponse(connection, Response{ID: request.ID, OK: false, Error: rpcError(err)})
			return
		}
		assignedActor = binding.Actor
		if s.attention != nil {
			for _, presence := range binding.SupersededPresences {
				s.attention.Cancel(presence.Actor, presence.RunID, presence.SessionID, bus.ErrPresenceSuperseded)
			}
		}
	}
	ready, _ := json.Marshal(map[string]interface{}{
		"protocol": ProtocolVersion, "daemon": "hollerd/0.1", "actor": assignedActor,
		"requested_actor": hello.Actor, "run_id": hello.RunID, "server_time": time.Now().UTC(), "build": s.build,
		"capabilities": []string{ActorAllocationCapability}, "minted": binding.Minted,
		"continuity_reclaimed": binding.ContinuityReclaimed, "provisional": binding.Provisional,
	})
	if err := writeResponse(connection, Response{ID: request.ID, OK: true, Result: ready}); err != nil {
		return
	}
	identity := Identity{
		Actor: assignedActor, RunID: hello.RunID, Client: hello.Client, Build: hello.Build,
		NameMode: hello.NameMode, ContinuityHandles: hello.ContinuityHandles, ProjectID: hello.ProjectID,
		Provisional: binding.Provisional,
	}
	defer func() {
		if identity.Provisional {
			_ = s.store.ReleaseProvisionalActor(context.Background(), identity.Actor)
		}
	}()
	for {
		request, err := readRequest(reader)
		if err != nil {
			return
		}
		if identity.Provisional {
			currentActor, bindingErr := s.store.CurrentActorForContinuity(connectionCtx, identity.ContinuityHandles)
			if bindingErr == nil && currentActor != identity.Actor {
				bindingErr = bus.ErrBindingReassigned
			}
			if bindingErr != nil {
				_ = writeResponse(connection, Response{ID: request.ID, OK: false, Error: rpcError(bindingErr)})
				continue
			}
			if finalizesProvisionalActor(request.Op) {
				if bindingErr := s.store.FinalizeActorAllocation(connectionCtx, identity.Actor, identity.RunID,
					identity.ProjectID, identity.ContinuityHandles); bindingErr != nil {
					_ = writeResponse(connection, Response{ID: request.ID, OK: false, Error: rpcError(bindingErr)})
					continue
				}
				identity.Provisional = false
			}
		}
		result, callErr := s.call(connectionCtx, identity, request.Op, request.Args)
		response := Response{ID: request.ID, OK: callErr == nil}
		if callErr != nil {
			response.Error = rpcError(callErr)
		} else {
			response.Result, callErr = json.Marshal(result)
			if callErr != nil {
				response.OK = false
				response.Error = rpcError(callErr)
			}
		}
		if err := writeResponse(connection, response); err != nil {
			return
		}
	}
}

func finalizesProvisionalActor(operation string) bool {
	switch operation {
	case "ping", "list_events", "who", "live_registrations", "heartbeat_registrations":
		return false
	default:
		return true
	}
}

func duplicateSocketDescriptor(connection net.Conn) (int, bool) {
	syscallConnection, ok := connection.(syscall.Conn)
	if !ok {
		return 0, false
	}
	raw, err := syscallConnection.SyscallConn()
	if err != nil {
		return 0, false
	}
	return fdliveness.Duplicate(raw)
}

func (s *Server) call(ctx context.Context, identity Identity, op string, raw json.RawMessage) (interface{}, error) {
	ctx = bus.WithCaller(ctx, bus.Caller{
		Actor: identity.Actor, RunID: identity.RunID, Client: identity.Client,
		BuildID: identity.Build.ID(), DaemonBuildID: s.build.ID(),
	})
	switch op {
	case "ping":
		return DaemonInfo{
			Protocol: ProtocolVersion, Actor: identity.Actor, PID: os.Getpid(), Build: s.build,
			Capabilities: []string{ActorAllocationCapability},
		}, nil
	case "send":
		var request bus.SendRequest
		if err := decodeStrict(raw, &request); err != nil {
			return nil, err
		}
		request.FromActor = identity.Actor
		request.FromRun = identity.RunID
		return s.store.Send(ctx, request)
	case "check_inbox":
		var args struct {
			Limit int `json:"limit"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.CheckInbox(ctx, identity.Actor, args.Limit)
	case "wait_attention":
		var args struct {
			SessionID string `json:"session_id"`
			Adapter   string `json:"adapter"`
			WaitNS    int64  `json:"wait_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		args.SessionID = strings.TrimSpace(args.SessionID)
		args.Adapter = strings.TrimSpace(args.Adapter)
		wait := time.Duration(args.WaitNS)
		if args.SessionID == "" {
			return nil, &bus.ValidationError{Field: "session_id", Problem: "is required"}
		}
		if err := bus.ValidateTextIdentifier("session_id", args.SessionID, 256); err != nil {
			return nil, err
		}
		if args.Adapter != "hook-long-poll" {
			return nil, &bus.ValidationError{Field: "adapter", Problem: "must be hook-long-poll"}
		}
		if wait <= 0 || wait > MaxAttentionWait {
			return nil, &bus.ValidationError{Field: "wait", Problem: "must be between 0 and 25s"}
		}
		if s.attention == nil {
			return nil, errors.New("attention waiting is unavailable")
		}
		waitCtx, cancel := context.WithTimeout(ctx, wait)
		defer cancel()
		notice, err := s.attention.Wait(waitCtx, identity.Actor, identity.RunID, args.SessionID, args.Adapter)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return bus.AttentionNotice{}, nil
		}
		return notice, err
	case "monitor_attach":
		var args struct {
			SessionID string `json:"session_id"`
			Adapter   string `json:"adapter"`
			LeaseNS   int64  `json:"lease_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		args.SessionID = strings.TrimSpace(args.SessionID)
		args.Adapter = strings.TrimSpace(args.Adapter)
		if s.attention == nil {
			return nil, errors.New("attention waiting is unavailable")
		}
		registration, err := s.store.AttachMonitor(ctx, identity.Actor, identity.RunID, args.SessionID,
			"claude", args.Adapter, time.Duration(args.LeaseNS))
		if err != nil {
			return nil, err
		}
		if err := s.attention.Attach(identity.Actor, identity.RunID, args.SessionID, func() error {
			return s.store.RearmAcceptedNotifications(ctx, identity.Actor)
		}); err != nil {
			return nil, err
		}
		registration.DeliveryHandle = ""
		return registration, nil
	case "claim":
		var args struct {
			MessageID string `json:"message_id"`
			LeaseNS   int64  `json:"lease_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.Claim(ctx, identity.Actor, args.MessageID, time.Duration(args.LeaseNS))
	case "ack":
		var args struct {
			MessageID  string `json:"message_id"`
			LeaseToken string `json:"lease_token"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return map[string]bool{"acked": true}, s.store.Ack(ctx, identity.Actor, args.MessageID, args.LeaseToken)
	case "extend":
		var args struct {
			MessageID  string `json:"message_id"`
			LeaseToken string `json:"lease_token"`
			LeaseNS    int64  `json:"lease_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.Extend(ctx, identity.Actor, args.MessageID, args.LeaseToken, time.Duration(args.LeaseNS))
	case "nack":
		var args struct {
			MessageID  string `json:"message_id"`
			LeaseToken string `json:"lease_token"`
			Reason     string `json:"reason"`
			Final      bool   `json:"final"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return map[string]bool{"nacked": true}, s.store.Nack(ctx, identity.Actor, args.MessageID, args.LeaseToken, args.Reason, args.Final)
	case "list_events":
		var args struct {
			Partition string `json:"partition"`
			Stream    string `json:"stream"`
			After     int64  `json:"after"`
			Limit     int    `json:"limit"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.ListEvents(ctx, args.Partition, args.Stream, args.After, args.Limit)
	case "set_actor_profile":
		var args struct {
			ProjectID string   `json:"project_id"`
			RoleText  string   `json:"role_text"`
			Accepts   []string `json:"accepts"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.SetActorProfile(ctx, identity.Actor, identity.RunID, args.ProjectID, bus.ActorProfileRequest{
			RoleText: args.RoleText, Accepts: args.Accepts,
		})
	case "who":
		var args struct {
			Limit int `json:"limit"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.Who(ctx, args.Limit)
	case "adopt_actor":
		var args struct {
			SourceActor    string `json:"source_actor"`
			ProjectID      string `json:"project_id"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.AdoptActor(ctx, bus.AdoptRequest{
			SourceActor: args.SourceActor, AdoptingActor: identity.Actor, AdoptingRun: identity.RunID,
			ProjectID: args.ProjectID, IdempotencyKey: args.IdempotencyKey,
		})
	case "register_session":
		var request bus.RegistrationRequest
		if err := decodeStrict(raw, &request); err != nil {
			return nil, err
		}
		request.Actor = identity.Actor
		request.RunID = identity.RunID
		return s.store.RegisterSession(ctx, request)
	case "live_registrations":
		var args struct {
			Actor string `json:"actor"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		args.Actor = strings.TrimSpace(args.Actor)
		if args.Actor != identity.Actor {
			return nil, &bus.ValidationError{Field: "actor", Problem: "does not match the authenticated API session"}
		}
		registrations, err := s.store.LiveRegistrations(ctx, identity.Actor)
		if err != nil {
			return nil, err
		}
		// Delivery handles are daemon-internal routing capabilities. In
		// particular, an OpenCode handle contains the loopback HTTP
		// credential for one live session. External clients only need the
		// registration identity and liveness metadata.
		for index := range registrations {
			registrations[index].DeliveryHandle = ""
		}
		return registrations, nil
	case "record_hydration":
		var args struct {
			ProjectID string `json:"project_id"`
			RunID     string `json:"run_id"`
			Harness   string `json:"harness"`
			SessionID string `json:"session_id"`
			Unread    int    `json:"unread"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return map[string]bool{"recorded": true}, s.store.RecordHydration(ctx, args.ProjectID, identity.Actor, identity.RunID, args.Harness, args.SessionID, args.Unread)
	case "expire_registration":
		var args struct {
			SessionID string `json:"session_id"`
			Reason    string `json:"reason"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if err := s.store.ExpireRegistration(ctx, identity.Actor, identity.RunID, args.SessionID, args.Reason); err != nil {
			return nil, err
		}
		if s.attention != nil {
			s.attention.Cancel(identity.Actor, identity.RunID, args.SessionID, bus.ErrSessionEnded)
		}
		return map[string]bool{"expired": true}, nil
	case "heartbeat_registrations":
		var args struct {
			LeaseNS int64 `json:"lease_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		count, err := s.store.HeartbeatRegistrations(ctx, identity.Actor, identity.RunID, time.Duration(args.LeaseNS))
		return map[string]int{"renewed": count}, err
	default:
		return nil, &bus.ValidationError{Field: "op", Problem: "is not supported: " + op}
	}
}

type Client struct {
	connection    net.Conn
	connectionMu  sync.Mutex
	reader        *bufio.Reader
	socketPath    string
	identity      Identity
	helloIdentity Identity
	serverBuild   buildinfo.Info
	mu            sync.Mutex
	nextID        uint64
}

func Dial(ctx context.Context, socketPath string, identity Identity) (*Client, error) {
	identity.Actor = strings.TrimSpace(identity.Actor)
	identity.RunID = strings.TrimSpace(identity.RunID)
	identity.Client = strings.TrimSpace(identity.Client)
	identity.ProjectID = strings.TrimSpace(identity.ProjectID)
	identity.NameMode = bus.NameMode(strings.TrimSpace(string(identity.NameMode)))
	if identity.Build.Commit == "" {
		identity.Build = buildinfo.Current()
	}
	client := &Client{socketPath: socketPath, identity: identity, helloIdentity: identity}
	bounded, cancel := withDefaultTimeout(ctx, 3*time.Second)
	defer cancel()
	client.mu.Lock()
	err := client.connectLocked(bounded)
	client.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) connectLocked(ctx context.Context) error {
	err := c.connectAttemptLocked(ctx, true)
	if err != nil && strings.Contains(err.Error(), `unknown field "build"`) {
		// Protocol v1 originally used strict hello decoding before build metadata
		// was added. Fall back once so a new client can still operate during a
		// daemon-first rolling upgrade. The legacy daemon reports no build and
		// therefore cannot produce READY certification evidence.
		return c.connectAttemptLocked(ctx, false)
	}
	return err
}

func (c *Client) connectAttemptLocked(ctx context.Context, includeBuild bool) error {
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("connect to hollerd at %s: %w", c.socketPath, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	c.connectionMu.Lock()
	c.connection = connection
	c.connectionMu.Unlock()
	c.reader = bufio.NewReader(connection)
	c.nextID = 1
	hello := map[string]interface{}{
		"protocol": ProtocolVersion, "client": c.helloIdentity.Client,
		"actor": c.helloIdentity.Actor, "run_id": c.helloIdentity.RunID,
	}
	featureIdentity := c.helloIdentity.NameMode != "" || len(c.helloIdentity.ContinuityHandles) > 0 || c.helloIdentity.Takeover
	if featureIdentity {
		hello["capabilities"] = []string{ActorAllocationCapability}
		hello["name_mode"] = c.helloIdentity.NameMode
		hello["continuity_handles"] = c.helloIdentity.ContinuityHandles
		hello["project_id"] = c.helloIdentity.ProjectID
		hello["takeover"] = c.helloIdentity.Takeover
	}
	if includeBuild {
		hello["build"] = c.helloIdentity.Build
	}
	request := Request{ID: c.nextID, Op: "hello"}
	request.Args, err = json.Marshal(hello)
	if err == nil {
		err = writeRequest(connection, request)
	}
	var response Response
	if err == nil {
		response, err = readResponse(c.reader)
	}
	_ = connection.SetDeadline(time.Time{})
	if err != nil {
		c.closeLocked()
		return fmt.Errorf("hollerd hello: %w", err)
	}
	if response.ID != request.ID || !response.OK {
		c.closeLocked()
		if response.Error != nil {
			return errorFromRPC(response.Error)
		}
		return errors.New("hollerd returned invalid hello response")
	}
	var ready struct {
		Protocol     int            `json:"protocol"`
		Actor        string         `json:"actor"`
		Build        buildinfo.Info `json:"build"`
		Capabilities []string       `json:"capabilities"`
		Provisional  bool           `json:"provisional"`
	}
	if err := json.Unmarshal(response.Result, &ready); err != nil {
		c.closeLocked()
		return fmt.Errorf("decode hollerd hello: %w", err)
	}
	if ready.Protocol != ProtocolVersion || ready.Actor == "" || (!featureIdentity && ready.Actor != c.helloIdentity.Actor) {
		c.closeLocked()
		return errors.New("hollerd returned mismatched session identity")
	}
	if featureIdentity && !containsString(ready.Capabilities, ActorAllocationCapability) {
		c.closeLocked()
		return errors.New("hollerd does not support negotiated actor allocation")
	}
	c.identity.Actor = ready.Actor
	c.identity.Provisional = ready.Provisional
	c.serverBuild = ready.Build
	return nil
}

func (c *Client) Identity() Identity {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.identity
}

func (c *Client) BoundIdentity() (string, string) {
	identity := c.Identity()
	return identity.Actor, identity.RunID
}

func (c *Client) closeLocked() {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	if c.connection != nil {
		_ = c.connection.Close()
	}
	c.connection = nil
	c.reader = nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
	return nil
}

// Interrupt closes the current transport without waiting for an in-flight
// long poll. The next API call reconnects normally.
func (c *Client) Interrupt() {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	if c.connection != nil {
		_ = c.connection.Close()
	}
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.DaemonInfo(ctx)
	return err
}

func (c *Client) DaemonInfo(ctx context.Context) (DaemonInfo, error) {
	var result DaemonInfo
	err := c.call(ctx, "ping", struct{}{}, &result)
	return result, err
}

func (c *Client) ServerBuild() buildinfo.Info {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serverBuild
}

func (c *Client) Send(ctx context.Context, request bus.SendRequest) (bus.SendResult, error) {
	request.FromActor = ""
	request.FromRun = ""
	var result bus.SendResult
	err := c.call(ctx, "send", request, &result)
	return result, err
}

func (c *Client) CheckInbox(ctx context.Context, actor string, limit int) ([]bus.InboxItem, error) {
	if err := c.requireActor(actor); err != nil {
		return nil, err
	}
	var result []bus.InboxItem
	err := c.call(ctx, "check_inbox", map[string]interface{}{"limit": limit}, &result)
	return result, err
}

func (c *Client) WaitAttention(ctx context.Context, actor, runID, sessionID, adapter string, wait time.Duration) (bus.AttentionNotice, error) {
	if err := c.requireActor(actor); err != nil {
		return bus.AttentionNotice{}, err
	}
	if err := c.requireRun(runID); err != nil {
		return bus.AttentionNotice{}, &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	waitCtx, cancel := withDefaultTimeout(ctx, wait+2*time.Second)
	defer cancel()
	var notice bus.AttentionNotice
	err := c.call(waitCtx, "wait_attention", map[string]interface{}{
		"session_id": sessionID, "adapter": adapter, "wait_ns": int64(wait),
	}, &notice)
	return notice, err
}

func (c *Client) MonitorAttach(ctx context.Context, actor, runID, sessionID, adapter string, lease time.Duration) (bus.Registration, error) {
	if err := c.requireActor(actor); err != nil {
		return bus.Registration{}, err
	}
	if err := c.requireRun(runID); err != nil {
		return bus.Registration{}, &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	var registration bus.Registration
	err := c.call(ctx, "monitor_attach", map[string]interface{}{
		"session_id": sessionID, "adapter": adapter, "lease_ns": int64(lease),
	}, &registration)
	return registration, err
}

func (c *Client) Claim(ctx context.Context, actor, messageID string, lease time.Duration) (bus.Claim, error) {
	if err := c.requireActor(actor); err != nil {
		return bus.Claim{}, err
	}
	var result bus.Claim
	err := c.call(ctx, "claim", map[string]interface{}{"message_id": messageID, "lease_ns": int64(lease)}, &result)
	return result, err
}

func (c *Client) Ack(ctx context.Context, actor, messageID, leaseToken string) error {
	if err := c.requireActor(actor); err != nil {
		return err
	}
	return c.call(ctx, "ack", map[string]interface{}{"message_id": messageID, "lease_token": leaseToken}, &struct{}{})
}

func (c *Client) Extend(ctx context.Context, actor, messageID, leaseToken string, lease time.Duration) (bus.LeaseExtension, error) {
	if err := c.requireActor(actor); err != nil {
		return bus.LeaseExtension{}, err
	}
	var result bus.LeaseExtension
	err := c.call(ctx, "extend", map[string]interface{}{
		"message_id": messageID, "lease_token": leaseToken, "lease_ns": int64(lease),
	}, &result)
	return result, err
}

func (c *Client) Nack(ctx context.Context, actor, messageID, leaseToken, reason string, final bool) error {
	if err := c.requireActor(actor); err != nil {
		return err
	}
	return c.call(ctx, "nack", map[string]interface{}{
		"message_id": messageID, "lease_token": leaseToken, "reason": reason, "final": final,
	}, &struct{}{})
}

func (c *Client) ListEvents(ctx context.Context, partition, stream string, after int64, limit int) ([]bus.Event, error) {
	var result []bus.Event
	err := c.call(ctx, "list_events", map[string]interface{}{
		"partition": partition, "stream": stream, "after": after, "limit": limit,
	}, &result)
	return result, err
}

func (c *Client) SetActorProfile(ctx context.Context, actor, runID, projectID string, request bus.ActorProfileRequest) (bus.ActorProfileResult, error) {
	if err := c.requireActor(actor); err != nil {
		return bus.ActorProfileResult{}, err
	}
	if err := c.requireRun(runID); err != nil {
		return bus.ActorProfileResult{}, &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	var result bus.ActorProfileResult
	err := c.call(ctx, "set_actor_profile", map[string]interface{}{
		"project_id": projectID, "role_text": request.RoleText, "accepts": request.Accepts,
	}, &result)
	return result, err
}

func (c *Client) Who(ctx context.Context, limit int) (bus.ActorDirectory, error) {
	var result bus.ActorDirectory
	err := c.call(ctx, "who", map[string]interface{}{"limit": limit}, &result)
	return result, err
}

func (c *Client) AdoptActor(ctx context.Context, request bus.AdoptRequest) (bus.AdoptResult, error) {
	var result bus.AdoptResult
	err := c.call(ctx, "adopt_actor", map[string]interface{}{
		"source_actor": request.SourceActor, "project_id": request.ProjectID, "idempotency_key": request.IdempotencyKey,
	}, &result)
	return result, err
}

func (c *Client) RegisterSession(ctx context.Context, request bus.RegistrationRequest) (bus.Registration, error) {
	if err := c.requireActor(request.Actor); err != nil {
		return bus.Registration{}, err
	}
	request.Actor, request.RunID = "", ""
	var result bus.Registration
	err := c.call(ctx, "register_session", request, &result)
	return result, err
}

func (c *Client) LiveRegistrations(ctx context.Context, actor string) ([]bus.Registration, error) {
	if err := c.requireActor(actor); err != nil {
		return nil, err
	}
	var result []bus.Registration
	err := c.call(ctx, "live_registrations", map[string]interface{}{"actor": actor}, &result)
	return result, err
}

func (c *Client) RecordHydration(ctx context.Context, projectID, actor, runID, harness, sessionID string, unread int) error {
	if err := c.requireActor(actor); err != nil {
		return err
	}
	if err := c.requireRun(runID); err != nil {
		return &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	return c.call(ctx, "record_hydration", map[string]interface{}{
		"project_id": projectID, "run_id": runID, "harness": harness, "session_id": sessionID, "unread": unread,
	}, &struct{}{})
}

func (c *Client) ExpireRegistration(ctx context.Context, actor, runID, sessionID, reason string) error {
	if err := c.requireActor(actor); err != nil {
		return err
	}
	if err := c.requireRun(runID); err != nil {
		return &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	return c.call(ctx, "expire_registration", map[string]interface{}{
		"session_id": sessionID, "reason": reason,
	}, &struct{}{})
}

func (c *Client) HeartbeatRegistrations(ctx context.Context, actor, runID string, lease time.Duration) (int, error) {
	if err := c.requireActor(actor); err != nil {
		return 0, err
	}
	if err := c.requireRun(runID); err != nil {
		return 0, &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	var result struct {
		Renewed int `json:"renewed"`
	}
	err := c.call(ctx, "heartbeat_registrations", map[string]interface{}{"lease_ns": int64(lease)}, &result)
	return result.Renewed, err
}

func (c *Client) requireActor(actor string) error {
	identity := c.Identity()
	if strings.TrimSpace(actor) != identity.Actor {
		return &bus.ValidationError{Field: "actor", Problem: "does not match the authenticated API session"}
	}
	return nil
}

func (c *Client) requireRun(runID string) error {
	identity := c.Identity()
	if strings.TrimSpace(runID) != identity.RunID {
		return &bus.ValidationError{Field: "run_id", Problem: "does not match the authenticated API session"}
	}
	return nil
}

func (c *Client) call(ctx context.Context, op string, args interface{}, target interface{}) error {
	ctx, cancel := withDefaultTimeout(ctx, 5*time.Second)
	defer cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection == nil {
		if err := c.connectLocked(ctx); err != nil {
			return err
		}
	}
	err := c.callOnceLocked(ctx, op, args, target)
	if err == nil {
		if c.identity.Provisional && finalizesProvisionalActor(op) {
			c.identity.Provisional = false
		}
		return nil
	}
	var responseErr *rpcResponseError
	if errors.As(err, &responseErr) {
		if errors.Is(err, bus.ErrBindingReassigned) && ctx.Err() == nil {
			c.closeLocked()
			if reconnectErr := c.connectLocked(ctx); reconnectErr != nil {
				return reconnectErr
			}
			retryErr := c.callOnceLocked(ctx, op, args, target)
			if retryErr == nil && c.identity.Provisional && finalizesProvisionalActor(op) {
				c.identity.Provisional = false
			}
			return retryErr
		}
		return err
	}
	c.closeLocked()
	if !retryAfterReconnect(op) || ctx.Err() != nil {
		return err
	}
	if err := c.connectLocked(ctx); err != nil {
		return err
	}
	retryErr := c.callOnceLocked(ctx, op, args, target)
	if retryErr == nil && c.identity.Provisional && finalizesProvisionalActor(op) {
		c.identity.Provisional = false
	}
	return retryErr
}

func (c *Client) callOnceLocked(ctx context.Context, op string, args interface{}, target interface{}) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.connection.SetDeadline(deadline)
		defer c.connection.SetDeadline(time.Time{})
	}
	c.nextID++
	raw, err := json.Marshal(args)
	if err != nil {
		return err
	}
	if err := writeRequest(c.connection, Request{ID: c.nextID, Op: op, Args: raw}); err != nil {
		return fmt.Errorf("write %s request: %w", op, err)
	}
	response, err := readResponse(c.reader)
	if err != nil {
		return fmt.Errorf("read %s response: %w", op, err)
	}
	if response.ID != c.nextID {
		return errors.New("hollerd returned mismatched request id")
	}
	if !response.OK {
		return &rpcResponseError{err: errorFromRPC(response.Error)}
	}
	if target == nil || len(response.Result) == 0 {
		return nil
	}
	if err := json.Unmarshal(response.Result, target); err != nil {
		return fmt.Errorf("decode %s result: %w", op, err)
	}
	return nil
}

func retryAfterReconnect(op string) bool {
	switch op {
	case "ping", "send", "check_inbox", "ack", "extend", "list_events", "who", "live_registrations", "monitor_attach", "expire_registration", "heartbeat_registrations":
		return true
	default:
		return false
	}
}

func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func writeRequest(writer io.Writer, request Request) error    { return writeFrame(writer, request) }
func writeResponse(writer io.Writer, response Response) error { return writeFrame(writer, response) }

func writeFrame(writer io.Writer, value interface{}) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > MaxFrameBytes {
		return errors.New("API frame exceeds 2 MiB")
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	if err := writeAll(writer, length[:]); err != nil {
		return err
	}
	return writeAll(writer, payload)
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func readRequest(reader io.Reader) (Request, error) {
	var request Request
	err := readFrame(reader, &request)
	return request, err
}

func readResponse(reader io.Reader) (Response, error) {
	var response Response
	err := readFrame(reader, &response)
	return response, err
}

func readFrame(reader io.Reader, target interface{}) error {
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(length[:])
	if size == 0 || size > MaxFrameBytes {
		return fmt.Errorf("invalid API frame length %d", size)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return err
	}
	return decodeStrict(payload, target)
}

func decodeStrict(raw []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func failure(id uint64, code, message string, retryable bool) Response {
	return Response{ID: id, OK: false, Error: &RPCError{Code: code, Message: message, Retryable: retryable}}
}

func rpcError(err error) *RPCError {
	code := "internal"
	switch {
	case errors.Is(err, bus.ErrInvalid):
		code = "invalid_request"
	case errors.Is(err, bus.ErrNotFound):
		code = "not_found"
	case errors.Is(err, bus.ErrNoMessage):
		code = "no_message"
	case errors.Is(err, bus.ErrAttentionWaiterBusy):
		code = "attention_waiter_busy"
	case errors.Is(err, bus.ErrSessionEnded):
		code = "session_ended"
	case errors.Is(err, bus.ErrPresenceSuperseded):
		code = "presence_superseded"
	case errors.Is(err, bus.ErrRegistrationExpired):
		return &RPCError{Code: "registration_expired", Message: err.Error(), Retryable: true}
	case errors.Is(err, bus.ErrIdempotencyConflict):
		code = "idempotency_conflict"
	case errors.Is(err, bus.ErrLeaseTokenMismatch):
		code = "lease_token_mismatch"
	case errors.Is(err, bus.ErrLeaseExpired):
		code = "lease_expired"
	case errors.Is(err, bus.ErrDeliveryTerminal):
		code = "delivery_terminal"
	case errors.Is(err, bus.ErrActorLive):
		code = "actor_live"
	case errors.Is(err, bus.ErrBindingStale):
		code = "binding_stale"
	case errors.Is(err, bus.ErrContinuityConflict):
		code = "continuity_conflict"
	case errors.Is(err, bus.ErrBindingReassigned):
		return &RPCError{Code: "binding_reassigned", Message: err.Error(), Retryable: true}
	case errors.Is(err, bus.ErrAdoptionConflict):
		code = "adoption_conflict"
	case errors.Is(err, bus.ErrAdoptionBusy):
		code = "adoption_busy"
	case errors.Is(err, bus.ErrActorNotLive):
		code = "actor_not_live"
	}
	return &RPCError{Code: code, Message: err.Error(), Retryable: false}
}

func errorFromRPC(rpc *RPCError) error {
	if rpc == nil {
		return errors.New("hollerd returned an empty error")
	}
	var sentinel error
	switch rpc.Code {
	case "invalid_request":
		sentinel = bus.ErrInvalid
	case "not_found":
		sentinel = bus.ErrNotFound
	case "no_message":
		sentinel = bus.ErrNoMessage
	case "attention_waiter_busy":
		sentinel = bus.ErrAttentionWaiterBusy
	case "session_ended":
		sentinel = bus.ErrSessionEnded
	case "presence_superseded":
		sentinel = bus.ErrPresenceSuperseded
	case "registration_expired":
		sentinel = bus.ErrRegistrationExpired
	case "idempotency_conflict":
		sentinel = bus.ErrIdempotencyConflict
	case "lease_token_mismatch":
		sentinel = bus.ErrLeaseTokenMismatch
	case "lease_expired":
		sentinel = bus.ErrLeaseExpired
	case "delivery_terminal":
		sentinel = bus.ErrDeliveryTerminal
	case "actor_live":
		sentinel = bus.ErrActorLive
	case "binding_stale":
		sentinel = bus.ErrBindingStale
	case "continuity_conflict":
		sentinel = bus.ErrContinuityConflict
	case "binding_reassigned":
		sentinel = bus.ErrBindingReassigned
	case "adoption_conflict":
		sentinel = bus.ErrAdoptionConflict
	case "adoption_busy":
		sentinel = bus.ErrAdoptionBusy
	case "actor_not_live":
		sentinel = bus.ErrActorNotLive
	}
	if sentinel != nil {
		return fmt.Errorf("%s: %w", rpc.Message, sentinel)
	}
	return errors.New(rpc.Message)
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
