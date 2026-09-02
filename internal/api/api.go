package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/fdliveness"
)

const (
	ProtocolVersion              = 1
	MaxFrameBytes                = 2 << 20
	MaxAttentionWait             = 25 * time.Second
	ActorAllocationCapability    = "actor-allocation-v1"
	ActorAliasCapability         = "actor-alias-v1"
	TypedRoutesCapability        = "typed-routes-v1"
	AliasClaimCapability         = "alias-claim-if-absent-v1"
	HarnessInstanceCapability    = "harness-instance-v1"
	OperatorConditionsCapability = "operator-conditions-v1"
	ActorLifecycleCapability     = "actor-lifecycle-v1"
)

func protocolCapabilities() []string {
	return []string{ActorAllocationCapability, ActorAliasCapability, TypedRoutesCapability, AliasClaimCapability, HarnessInstanceCapability, OperatorConditionsCapability, ActorLifecycleCapability, CapabilityBridgeCapability}
}

type Identity struct {
	Actor              string
	RunID              string
	Client             string
	Build              buildinfo.Info
	NameMode           bus.NameMode
	ContinuityHandles  []string
	ProjectID          string
	Harness            string
	HarnessInstance    string
	InstanceState      string
	Takeover           bool
	Provisional        bool
	AdoptedPredecessor string
	PendingPredecessor string
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
	WhoIncludingArchived(context.Context, int) (bus.ActorDirectory, error)
	ArchivePreflight(context.Context, string, int) (bus.ActorArchivePreflight, error)
	ArchiveActor(context.Context, string, string, bool) (bus.ActorArchiveResult, error)
	RestoreActor(context.Context, string, string) (bus.ActorArchiveResult, error)
	RevokeDeliveryLease(context.Context, string, string, time.Duration) error
	AdoptActor(context.Context, bus.AdoptRequest) (bus.AdoptResult, error)
	BindActor(context.Context, bus.ActorBindRequest) (bus.ActorBindResult, error)
	FinalizeActorAllocation(context.Context, string, string, string, []string) error
	CurrentActorForContinuity(context.Context, []string) (string, error)
	ReleaseProvisionalActor(context.Context, string) error
	ReserveActorName(context.Context, string) error
	ClaimAliasIfAbsent(context.Context, bus.AliasClaimRequest) (bus.AliasClaimResult, error)
	SetAlias(context.Context, bus.AliasSetRequest) (bus.AliasMutationResult, error)
	RemoveAlias(context.Context, bus.AliasRemoveRequest) (bus.AliasMutationResult, error)
	ListAliases(context.Context) ([]bus.ActorAlias, error)
	ResolveAlias(context.Context, string) (bus.ActorAlias, error)
	AliasPreflight(context.Context, string, string) (bus.AliasPreflight, error)
	DeliveryReceipts(context.Context, bus.Message) ([]bus.DeliveryReceipt, error)
	ObserveCondition(context.Context, bus.ConditionObservation) (bus.OperatorCondition, error)
	ResolveCondition(context.Context, string, string) error
	ResolveConditionIfReason(context.Context, string, string, string) error
	ListConditions(context.Context, bool, int) ([]bus.OperatorCondition, error)
	AcknowledgeCondition(context.Context, string, string, int) (bus.OperatorCondition, error)
	SnoozeCondition(context.Context, string, string, int, time.Time) (bus.OperatorCondition, error)
	ClaimConditionPresentation(context.Context, string, string, int, string, time.Duration) (bool, error)
}

type AttentionBroker interface {
	Wait(context.Context, string, string, string, string) (bus.AttentionNotice, error)
	Attach(string, string, string, func() error) error
	Cancel(string, string, string, error)
}

type attentionAttachmentInspector interface {
	Attached(string, string, string) bool
}

type Server struct {
	store                  Store
	build                  buildinfo.Info
	attention              AttentionBroker
	capabilities           map[string]registeredCapability
	capabilityErr          error
	resolveHarnessInstance HarnessInstanceResolver
}

type ServerOption func(*Server)

type HarnessInstanceResolver func(net.Conn, string) (string, error)

func WithHarnessInstanceResolver(resolver HarnessInstanceResolver) ServerOption {
	return func(server *Server) { server.resolveHarnessInstance = resolver }
}

func NewServer(store Store, options ...ServerOption) *Server {
	server := &Server{
		store: store, build: buildinfo.Current(),
		capabilities: make(map[string]registeredCapability), resolveHarnessInstance: localHarnessInstance,
	}
	server.installDefaultCapabilities()
	for _, option := range options {
		option(server)
	}
	return server
}

func localHarnessInstance(connection net.Conn, harness string) (string, error) {
	harness = strings.ToLower(strings.TrimSpace(harness))
	switch harness {
	case "claude", "codex", "opencode":
	default:
		return "", &bus.ValidationError{Field: "harness", Problem: "must be claude, codex, or opencode"}
	}
	pid, err := peerProcessID(connection)
	if err != nil {
		return "", err
	}
	const maximumAncestorDepth = 64
	for depth := 0; pid > 1 && depth < maximumAncestorDepth; depth++ {
		parent, command, err := processIdentity(pid)
		if err != nil {
			return "", err
		}
		name := strings.ToLower(filepath.Base(command))
		if strings.Contains(name, harness) && !strings.Contains(name, "holler") {
			start, err := processStart(pid)
			if err != nil {
				return "", err
			}
			digest := sha256.Sum256([]byte(harness + "\x00" + strconv.Itoa(pid) + "\x00" + start))
			return "hin_" + hex.EncodeToString(digest[:16]), nil
		}
		pid = parent
	}
	return "", fmt.Errorf("could not prove a live %s harness ancestor", harness)
}

func processIdentity(pid int) (int, string, error) {
	output, err := exec.Command("/bin/ps", "-o", "ppid=", "-o", "comm=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, "", fmt.Errorf("inspect process %d: %w", pid, err)
	}
	fields := strings.Fields(strings.TrimSpace(string(output)))
	if len(fields) < 2 {
		return 0, "", fmt.Errorf("process %d identity is unavailable", pid)
	}
	parent, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, "", fmt.Errorf("decode parent process for %d: %w", pid, err)
	}
	return parent, strings.Join(fields[1:], " "), nil
}

func processStart(pid int) (string, error) {
	output, err := exec.Command("/bin/ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", fmt.Errorf("inspect process start for %d: %w", pid, err)
	}
	start := strings.TrimSpace(string(output))
	if start == "" {
		return "", fmt.Errorf("process %d start fingerprint is unavailable", pid)
	}
	return start, nil
}

func WithAttentionBroker(broker AttentionBroker) ServerOption {
	return func(server *Server) { server.attention = broker }
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if s.capabilityErr != nil {
		return fmt.Errorf("configure daemon capabilities: %w", s.capabilityErr)
	}
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
		Harness           string         `json:"harness"`
		Takeover          bool           `json:"takeover"`
	}
	if err := decodeStrict(request.Args, &hello); err != nil {
		_ = writeResponse(connection, failure(request.ID, "bad_request", err.Error(), false))
		return
	}
	hello.Actor = strings.TrimSpace(hello.Actor)
	hello.RunID = strings.TrimSpace(hello.RunID)
	hello.ProjectID = strings.TrimSpace(hello.ProjectID)
	hello.Harness = strings.ToLower(strings.TrimSpace(hello.Harness))
	if hello.ProjectID == "" {
		hello.ProjectID = "default"
	}
	for index := range hello.ContinuityHandles {
		hello.ContinuityHandles[index] = strings.TrimSpace(hello.ContinuityHandles[index])
		if strings.HasPrefix(hello.ContinuityHandles[index], "instance:") {
			_ = writeResponse(connection, failure(request.ID, "bad_request", "continuity_handles namespace instance is daemon-owned", false))
			return
		}
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
	featureIdentity := hello.NameMode != "" || len(hello.ContinuityHandles) > 0 || hello.Takeover
	instanceState := "legacy"
	harnessInstance := ""
	if hello.Harness != "" {
		if !containsString(hello.Capabilities, HarnessInstanceCapability) {
			_ = writeResponse(connection, failure(request.ID, "capability_required", "harness identity requires capability "+HarnessInstanceCapability, false))
			return
		}
		switch hello.Harness {
		case "claude", "codex", "opencode":
		default:
			_ = writeResponse(connection, failure(request.ID, "bad_request", "harness must be claude, codex, or opencode", false))
			return
		}
		instanceState = "unreconciled"
		if s.resolveHarnessInstance != nil {
			if resolved, resolveErr := s.resolveHarnessInstance(connection, hello.Harness); resolveErr == nil && strings.TrimSpace(resolved) != "" {
				harnessInstance = strings.TrimSpace(resolved)
				if featureIdentity && (hello.NameMode == bus.NameModeAllocate || hello.NameMode == bus.NameModeExact) {
					hello.ContinuityHandles = append(hello.ContinuityHandles, "instance:"+harnessInstance)
				}
				instanceState = "bound"
			}
		}
	}
	assignedActor := hello.Actor
	assignedRunID := hello.RunID
	var binding bus.ActorBindResult
	if featureIdentity {
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
			if errors.Is(err, bus.ErrContinuityConflict) {
				details, _ := json.Marshal(map[string]interface{}{
					"requested_actor": hello.Actor, "harness": hello.Harness,
					"harness_instance": harnessInstance, "continuity_kinds": continuityKinds(hello.ContinuityHandles),
				})
				subject := hello.Actor
				if harnessInstance != "" {
					subject = harnessInstance
				}
				_, _ = s.store.ObserveCondition(connectionCtx, bus.ConditionObservation{
					Kind: "identity_conflict", Subject: subject, ReasonCode: "contradictory_binding_evidence",
					Summary: "Conflicting connector identity evidence was rejected", Details: details,
				})
			}
			_ = writeResponse(connection, Response{ID: request.ID, OK: false, Error: rpcError(err)})
			return
		}
		assignedActor = binding.Actor
		if binding.AssignedRunID != "" {
			assignedRunID = binding.AssignedRunID
		}
		if len(binding.AcceptedContinuityHandles) > 0 {
			hello.ContinuityHandles = append([]string(nil), binding.AcceptedContinuityHandles...)
		}
		if s.attention != nil {
			for _, presence := range binding.SupersededPresences {
				s.attention.Cancel(presence.Actor, presence.RunID, presence.SessionID, bus.ErrPresenceSuperseded)
			}
		}
	} else if err := s.store.ReserveActorName(connectionCtx, assignedActor); err != nil {
		_ = writeResponse(connection, Response{ID: request.ID, OK: false, Error: rpcError(err)})
		return
	}
	if binding.PendingPredecessor != "" {
		details, _ := json.Marshal(map[string]interface{}{
			"predecessor": binding.PendingPredecessor, "successor": assignedActor,
			"harness": hello.Harness, "harness_instance": harnessInstance,
		})
		_, _ = s.store.ObserveCondition(connectionCtx, bus.ConditionObservation{
			Kind: "pending_takeover", Subject: binding.PendingPredecessor, ReasonCode: "predecessor_still_live",
			Summary: "A resumed harness is waiting for an explicit identity takeover", Details: details,
		})
	} else if binding.ContinuityReclaimed && hello.Takeover {
		_ = s.store.ResolveCondition(connectionCtx, "pending_takeover", assignedActor)
	}
	if hello.Harness != "" {
		if instanceState == "unreconciled" {
			details, _ := json.Marshal(map[string]string{"actor": assignedActor, "harness": hello.Harness})
			_, _ = s.store.ObserveCondition(connectionCtx, bus.ConditionObservation{
				Kind: "attention_unavailable", Subject: assignedActor, ReasonCode: "harness_instance_unreconciled",
				Summary: "Holler could not prove the live harness instance for " + assignedActor, Details: details,
			})
		} else if instanceState == "bound" {
			_ = s.store.ResolveConditionIfReason(connectionCtx, "attention_unavailable", assignedActor, "harness_instance_unreconciled")
		}
	}
	ready, _ := json.Marshal(map[string]interface{}{
		"protocol": ProtocolVersion, "daemon": "hollerd/0.1", "actor": assignedActor,
		"requested_actor": hello.Actor, "run_id": assignedRunID, "server_time": time.Now().UTC(), "build": s.build,
		"capabilities": protocolCapabilities(), "minted": binding.Minted,
		"continuity_reclaimed": binding.ContinuityReclaimed, "provisional": binding.Provisional,
		"adopted_predecessor": binding.AdoptedPredecessor, "harness_instance": harnessInstance,
		"pending_predecessor": binding.PendingPredecessor, "instance_state": instanceState,
	})
	if err := writeResponse(connection, Response{ID: request.ID, OK: true, Result: ready}); err != nil {
		return
	}
	identity := Identity{
		Actor: assignedActor, RunID: assignedRunID, Client: hello.Client, Build: hello.Build,
		NameMode: hello.NameMode, ContinuityHandles: hello.ContinuityHandles, ProjectID: hello.ProjectID,
		Harness: hello.Harness, HarnessInstance: harnessInstance, InstanceState: instanceState,
		Provisional:        binding.Provisional,
		AdoptedPredecessor: binding.AdoptedPredecessor,
		PendingPredecessor: binding.PendingPredecessor,
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
	case "ping", "list_events", "who", "list_aliases", "resolve_alias", "list_capabilities", "invoke_read_capability", "live_registrations", "heartbeat_registrations", "list_conditions":
		return false
	default:
		return true
	}
}

func continuityKinds(handles []string) []string {
	seen := make(map[string]struct{})
	for _, handle := range handles {
		kind := handle
		if index := strings.IndexByte(handle, ':'); index >= 0 {
			kind = handle[:index]
		}
		seen[kind] = struct{}{}
	}
	kinds := make([]string, 0, len(seen))
	for kind := range seen {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
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
			Capabilities: protocolCapabilities(),
		}, nil
	case "list_capabilities":
		var args struct{}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.listCapabilities(), nil
	case "list_conditions":
		var args struct {
			IncludeResolved bool `json:"include_resolved"`
			Limit           int  `json:"limit"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.ListConditions(ctx, args.IncludeResolved, args.Limit)
	case "acknowledge_condition":
		if identity.Actor != "operator" {
			return nil, &bus.ValidationError{Field: "actor", Problem: "only the operator identity may acknowledge conditions"}
		}
		var args struct {
			Kind       string `json:"kind"`
			Subject    string `json:"subject"`
			Generation int    `json:"generation"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.AcknowledgeCondition(ctx, args.Kind, args.Subject, args.Generation)
	case "snooze_condition":
		if identity.Actor != "operator" {
			return nil, &bus.ValidationError{Field: "actor", Problem: "only the operator identity may snooze conditions"}
		}
		var args struct {
			Kind       string    `json:"kind"`
			Subject    string    `json:"subject"`
			Generation int       `json:"generation"`
			Until      time.Time `json:"until"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.SnoozeCondition(ctx, args.Kind, args.Subject, args.Generation, args.Until)
	case "claim_condition_presentation":
		var args struct {
			Kind       string `json:"kind"`
			Subject    string `json:"subject"`
			Generation int    `json:"generation"`
			LeaseNS    int64  `json:"lease_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		claimed, err := s.store.ClaimConditionPresentation(ctx, args.Kind, args.Subject, args.Generation,
			identity.Actor+"/"+identity.RunID, time.Duration(args.LeaseNS))
		return map[string]bool{"claimed": claimed}, err
	case "invoke_read_capability", "invoke_write_capability":
		var invocation bus.CapabilityInvocation
		if err := decodeStrict(raw, &invocation); err != nil {
			return nil, err
		}
		expected := bus.CapabilityRead
		if op == "invoke_write_capability" {
			expected = bus.CapabilityWrite
		}
		return s.invokeCapability(ctx, identity, expected, invocation)
	case "send":
		var request bus.SendRequest
		if err := decodeStrict(raw, &request); err != nil {
			return nil, err
		}
		request.FromActor = identity.Actor
		request.FromRun = identity.RunID
		result, err := s.store.Send(ctx, request)
		if err != nil {
			return nil, err
		}
		return s.finalizeSend(ctx, result)
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
			return nil, bus.ErrAttentionUnavailable
		}
		if identity.InstanceState == "unreconciled" {
			return nil, fmt.Errorf("%w: daemon could not prove the live %s harness instance", bus.ErrAttentionUnavailable, identity.Harness)
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
			return nil, bus.ErrAttentionUnavailable
		}
		if identity.InstanceState == "unreconciled" {
			return nil, fmt.Errorf("%w: daemon could not prove the live %s harness instance", bus.ErrAttentionUnavailable, identity.Harness)
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
			Limit           int  `json:"limit"`
			IncludeArchived bool `json:"include_archived"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if args.IncludeArchived {
			return s.store.WhoIncludingArchived(ctx, args.Limit)
		}
		return s.store.Who(ctx, args.Limit)
	case "archive_preflight":
		var args struct {
			Actor string `json:"actor"`
			Limit int    `json:"limit"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.ArchivePreflight(ctx, args.Actor, args.Limit)
	case "archive_actor":
		if identity.Actor != "operator" {
			return nil, &bus.ValidationError{Field: "actor", Problem: "only the operator identity may archive actors"}
		}
		var args struct {
			Actor       string `json:"actor"`
			AllowUnread bool   `json:"allow_unread"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.ArchiveActor(ctx, args.Actor, identity.Actor, args.AllowUnread)
	case "restore_actor":
		if identity.Actor != "operator" {
			return nil, &bus.ValidationError{Field: "actor", Problem: "only the operator identity may restore actors"}
		}
		var args struct {
			Actor string `json:"actor"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.RestoreActor(ctx, args.Actor, identity.Actor)
	case "revoke_delivery_lease":
		if identity.Actor != "operator" {
			return nil, &bus.ValidationError{Field: "actor", Problem: "only the operator identity may revoke leases"}
		}
		var args struct {
			Actor        string `json:"actor"`
			MessageID    string `json:"message_id"`
			CrashGraceNS int64  `json:"crash_grace_ns"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return map[string]bool{"revoked": true}, s.store.RevokeDeliveryLease(ctx, args.Actor, args.MessageID, time.Duration(args.CrashGraceNS))
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
	case "set_alias":
		var args struct {
			Alias          string `json:"alias"`
			Actor          string `json:"actor"`
			ProjectID      string `json:"project_id"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.SetAlias(ctx, bus.AliasSetRequest{
			Alias: args.Alias, Actor: args.Actor, UpdatedByActor: identity.Actor,
			UpdatedByRun: identity.RunID, ProjectID: args.ProjectID, IdempotencyKey: args.IdempotencyKey,
		})
	case "claim_alias_if_absent":
		var args struct {
			Alias          string `json:"alias"`
			Actor          string `json:"actor"`
			PolicyID       string `json:"policy_id"`
			Harness        string `json:"harness"`
			ProjectID      string `json:"project_id"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		result, err := s.store.ClaimAliasIfAbsent(ctx, bus.AliasClaimRequest{
			Alias: args.Alias, Actor: args.Actor, PolicyID: args.PolicyID, Harness: args.Harness,
			UpdatedByActor: identity.Actor, UpdatedByRun: identity.RunID,
			ProjectID: args.ProjectID, IdempotencyKey: args.IdempotencyKey,
		})
		if err == nil && !result.Claimed && result.Alias.Actor != args.Actor {
			details, _ := json.Marshal(result)
			_, _ = s.store.ObserveCondition(ctx, bus.ConditionObservation{
				Kind: "alias_collision", Subject: args.Alias, ReasonCode: "claim_if_absent_lost",
				Summary: "Alias " + args.Alias + " is already owned by another actor", Details: details,
			})
		} else if err == nil && result.Alias.Actor == args.Actor {
			_ = s.store.ResolveCondition(ctx, "alias_collision", args.Alias)
		}
		return result, err
	case "remove_alias":
		var args struct {
			Alias          string `json:"alias"`
			ProjectID      string `json:"project_id"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.RemoveAlias(ctx, bus.AliasRemoveRequest{
			Alias: args.Alias, UpdatedByActor: identity.Actor, UpdatedByRun: identity.RunID,
			ProjectID: args.ProjectID, IdempotencyKey: args.IdempotencyKey,
		})
	case "list_aliases":
		var args struct{}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.ListAliases(ctx)
	case "resolve_alias":
		var args struct {
			Alias string `json:"alias"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.ResolveAlias(ctx, args.Alias)
	case "alias_preflight":
		var args struct {
			Alias         string `json:"alias"`
			ProposedActor string `json:"proposed_actor"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		return s.store.AliasPreflight(ctx, args.Alias, args.ProposedActor)
	case "register_session":
		var request bus.RegistrationRequest
		if err := decodeStrict(raw, &request); err != nil {
			return nil, err
		}
		request.Actor = identity.Actor
		request.RunID = identity.RunID
		if identity.InstanceState == "unreconciled" && request.AttentionMode != "" && request.AttentionMode != "startup-only" {
			request.AttentionMode = "startup-only"
			request.DeliveryHandle = ""
		}
		registration, err := s.store.RegisterSession(ctx, request)
		if err == nil && registration.AttentionMode != "startup-only" && identity.InstanceState != "unreconciled" {
			_ = s.store.ResolveConditionIfReason(ctx, "attention_unavailable", identity.Actor, "startup_only_selected")
		} else if err == nil && registration.AttentionMode == "startup-only" && identity.InstanceState == "bound" {
			details, _ := json.Marshal(map[string]string{"actor": identity.Actor, "harness": identity.Harness})
			_, _ = s.store.ObserveCondition(ctx, bus.ConditionObservation{
				Kind: "attention_unavailable", Subject: identity.Actor, ReasonCode: "startup_only_selected",
				Summary: "Automatic wake is disabled by configuration for " + identity.Actor, Details: details,
			})
		}
		return registration, err
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

func (s *Server) finalizeSend(ctx context.Context, result bus.SendResult) (bus.SendResult, error) {
	var err error
	result.DeliveryReceipts, err = s.store.DeliveryReceipts(ctx, result.Message)
	if err != nil {
		return bus.SendResult{}, err
	}
	if err := s.decorateAttentionReceipts(ctx, result.DeliveryReceipts); err != nil {
		return bus.SendResult{}, err
	}
	for _, receipt := range result.DeliveryReceipts {
		if receipt.AttentionCapability != "disabled_by_config" && receipt.AttentionCapability != "integration_missing" && receipt.AttentionCapability != "version_blocked" {
			continue
		}
		details, _ := json.Marshal(receipt)
		_, _ = s.store.ObserveCondition(ctx, bus.ConditionObservation{
			Kind: "attention_unavailable", Subject: receipt.CanonicalRecipient,
			ReasonCode: receipt.AttentionReason,
			Summary:    "Automatic wake is unavailable for " + receipt.CanonicalRecipient,
			Details:    details,
		})
	}
	return result, nil
}

func (s *Server) decorateAttentionReceipts(ctx context.Context, receipts []bus.DeliveryReceipt) error {
	inspector, ok := s.attention.(attentionAttachmentInspector)
	if !ok {
		return nil
	}
	for index := range receipts {
		if receipts[index].AttentionAttachment != "reconnecting" {
			continue
		}
		registrations, err := s.store.LiveRegistrations(ctx, receipts[index].CanonicalRecipient)
		if err != nil {
			return err
		}
		for _, registration := range registrations {
			if registration.Harness == "claude" && registration.AttentionMode == "hook-long-poll" &&
				inspector.Attached(registration.Actor, registration.RunID, registration.SessionID) {
				receipts[index].AttentionAttachment = "attached"
				receipts[index].AttentionReason = ""
				receipts[index].AttentionDetail = ""
				break
			}
		}
	}
	return nil
}

type Client struct {
	connection         net.Conn
	connectionMu       sync.Mutex
	reader             *bufio.Reader
	socketPath         string
	identity           Identity
	helloIdentity      Identity
	serverBuild        buildinfo.Info
	serverCapabilities []string
	mu                 sync.Mutex
	nextID             uint64
}

func Dial(ctx context.Context, socketPath string, identity Identity) (*Client, error) {
	identity.Actor = strings.TrimSpace(identity.Actor)
	identity.RunID = strings.TrimSpace(identity.RunID)
	identity.Client = strings.TrimSpace(identity.Client)
	identity.ProjectID = strings.TrimSpace(identity.ProjectID)
	identity.Harness = strings.ToLower(strings.TrimSpace(identity.Harness))
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
	includeProject := c.helloIdentity.ProjectID != ""
	err := c.connectAttemptLocked(ctx, true, includeProject)
	if err != nil && strings.Contains(err.Error(), `unknown field "build"`) {
		// Protocol v1 originally used strict hello decoding before build metadata
		// was added. Fall back without it so a new client can still operate during a
		// daemon-first rolling upgrade. The legacy daemon reports no build and
		// therefore cannot produce READY certification evidence.
		err = c.connectAttemptLocked(ctx, false, includeProject)
	}
	featureIdentity := c.helloIdentity.NameMode != "" || len(c.helloIdentity.ContinuityHandles) > 0 || c.helloIdentity.Takeover
	if err != nil && includeProject && !featureIdentity && strings.Contains(err.Error(), `unknown field "project_id"`) {
		// Project identity became part of protocol v1 after the oldest strict
		// daemon. That daemon has no capability bridge, so legacy sessions may
		// omit project context only to preserve their pre-bridge operations.
		return c.connectAttemptLocked(ctx, false, false)
	}
	return err
}

func (c *Client) connectAttemptLocked(ctx context.Context, includeBuild, includeProject bool) error {
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
	featureHarness := c.helloIdentity.Harness != ""
	if includeProject {
		hello["project_id"] = c.helloIdentity.ProjectID
	}
	if featureIdentity || featureHarness {
		hello["capabilities"] = []string{ActorAllocationCapability, ActorAliasCapability, TypedRoutesCapability, AliasClaimCapability, HarnessInstanceCapability, OperatorConditionsCapability, ActorLifecycleCapability}
	}
	if featureIdentity {
		hello["name_mode"] = c.helloIdentity.NameMode
		hello["continuity_handles"] = c.helloIdentity.ContinuityHandles
		hello["takeover"] = c.helloIdentity.Takeover
	}
	if featureHarness {
		hello["harness"] = c.helloIdentity.Harness
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
		Protocol           int            `json:"protocol"`
		Actor              string         `json:"actor"`
		RunID              string         `json:"run_id"`
		Build              buildinfo.Info `json:"build"`
		Capabilities       []string       `json:"capabilities"`
		Provisional        bool           `json:"provisional"`
		AdoptedPredecessor string         `json:"adopted_predecessor"`
		PendingPredecessor string         `json:"pending_predecessor"`
		HarnessInstance    string         `json:"harness_instance"`
		InstanceState      string         `json:"instance_state"`
	}
	if err := json.Unmarshal(response.Result, &ready); err != nil {
		c.closeLocked()
		return fmt.Errorf("decode hollerd hello: %w", err)
	}
	if ready.RunID == "" && !featureIdentity && !featureHarness {
		// The oldest protocol-v1 daemon did not echo run_id in READY. This is
		// safe only for the legacy path, where no daemon-owned identity binding
		// was negotiated and the authenticated run therefore cannot change.
		ready.RunID = c.helloIdentity.RunID
	}
	if ready.Protocol != ProtocolVersion || ready.Actor == "" || ready.RunID == "" || (!featureIdentity && ready.Actor != c.helloIdentity.Actor) {
		c.closeLocked()
		return errors.New("hollerd returned mismatched session identity")
	}
	if featureIdentity && !containsString(ready.Capabilities, ActorAllocationCapability) {
		c.closeLocked()
		return errors.New("hollerd does not support negotiated actor allocation")
	}
	c.identity.Actor = ready.Actor
	c.identity.RunID = ready.RunID
	c.identity.Provisional = ready.Provisional
	c.identity.AdoptedPredecessor = ready.AdoptedPredecessor
	c.identity.PendingPredecessor = ready.PendingPredecessor
	c.identity.HarnessInstance = ready.HarnessInstance
	c.identity.InstanceState = ready.InstanceState
	c.serverBuild = ready.Build
	c.serverCapabilities = append([]string(nil), ready.Capabilities...)
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
	if (len(request.Destinations) > 0 || (strings.TrimSpace(request.InReplyTo) != "" && len(request.ToActors) == 0)) &&
		!c.supports(TypedRoutesCapability) {
		return bus.SendResult{}, &bus.ValidationError{Field: "destinations", Problem: "connected daemon does not support typed routes; upgrade hollerd"}
	}
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
	err := c.call(ctx, "who", map[string]interface{}{"limit": limit, "include_archived": false}, &result)
	return result, err
}

func (c *Client) WhoIncludingArchived(ctx context.Context, limit int) (bus.ActorDirectory, error) {
	var result bus.ActorDirectory
	err := c.call(ctx, "who", map[string]interface{}{"limit": limit, "include_archived": true}, &result)
	return result, err
}

func (c *Client) ArchivePreflight(ctx context.Context, actor string, limit int) (bus.ActorArchivePreflight, error) {
	var result bus.ActorArchivePreflight
	err := c.call(ctx, "archive_preflight", map[string]interface{}{"actor": actor, "limit": limit}, &result)
	return result, err
}

func (c *Client) ArchiveActor(ctx context.Context, actor string, allowUnread bool) (bus.ActorArchiveResult, error) {
	var result bus.ActorArchiveResult
	err := c.call(ctx, "archive_actor", map[string]interface{}{"actor": actor, "allow_unread": allowUnread}, &result)
	return result, err
}

func (c *Client) RestoreActor(ctx context.Context, actor string) (bus.ActorArchiveResult, error) {
	var result bus.ActorArchiveResult
	err := c.call(ctx, "restore_actor", map[string]interface{}{"actor": actor}, &result)
	return result, err
}

func (c *Client) RevokeDeliveryLease(ctx context.Context, actor, messageID string, crashGrace time.Duration) error {
	return c.call(ctx, "revoke_delivery_lease", map[string]interface{}{
		"actor": actor, "message_id": messageID, "crash_grace_ns": int64(crashGrace),
	}, &struct{}{})
}

func (c *Client) AdoptActor(ctx context.Context, request bus.AdoptRequest) (bus.AdoptResult, error) {
	var result bus.AdoptResult
	err := c.call(ctx, "adopt_actor", map[string]interface{}{
		"source_actor": request.SourceActor, "project_id": request.ProjectID, "idempotency_key": request.IdempotencyKey,
	}, &result)
	return result, err
}

func (c *Client) SetAlias(ctx context.Context, request bus.AliasSetRequest) (bus.AliasMutationResult, error) {
	var result bus.AliasMutationResult
	err := c.call(ctx, "set_alias", map[string]interface{}{
		"alias": request.Alias, "actor": request.Actor, "project_id": request.ProjectID,
		"idempotency_key": request.IdempotencyKey,
	}, &result)
	return result, err
}

func (c *Client) ClaimAliasIfAbsent(ctx context.Context, request bus.AliasClaimRequest) (bus.AliasClaimResult, error) {
	if !c.supports(AliasClaimCapability) {
		return bus.AliasClaimResult{}, &bus.ValidationError{Field: "policy_id", Problem: "connected daemon does not support atomic alias claims; upgrade hollerd"}
	}
	var result bus.AliasClaimResult
	err := c.call(ctx, "claim_alias_if_absent", map[string]interface{}{
		"alias": request.Alias, "actor": request.Actor, "policy_id": request.PolicyID, "harness": request.Harness,
		"project_id": request.ProjectID, "idempotency_key": request.IdempotencyKey,
	}, &result)
	return result, err
}

func (c *Client) supports(capability string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return containsString(c.serverCapabilities, capability)
}

func (c *Client) RemoveAlias(ctx context.Context, request bus.AliasRemoveRequest) (bus.AliasMutationResult, error) {
	var result bus.AliasMutationResult
	err := c.call(ctx, "remove_alias", map[string]interface{}{
		"alias": request.Alias, "project_id": request.ProjectID, "idempotency_key": request.IdempotencyKey,
	}, &result)
	return result, err
}

func (c *Client) ListAliases(ctx context.Context) ([]bus.ActorAlias, error) {
	var result []bus.ActorAlias
	err := c.call(ctx, "list_aliases", struct{}{}, &result)
	return result, err
}

func (c *Client) ResolveAlias(ctx context.Context, alias string) (bus.ActorAlias, error) {
	var result bus.ActorAlias
	err := c.call(ctx, "resolve_alias", map[string]interface{}{"alias": alias}, &result)
	return result, err
}

func (c *Client) AliasPreflight(ctx context.Context, alias, proposedActor string) (bus.AliasPreflight, error) {
	var result bus.AliasPreflight
	err := c.call(ctx, "alias_preflight", map[string]interface{}{"alias": alias, "proposed_actor": proposedActor}, &result)
	return result, err
}

func (c *Client) ListCapabilities(ctx context.Context) ([]bus.CapabilityDescriptor, error) {
	var result []bus.CapabilityDescriptor
	err := c.call(ctx, "list_capabilities", struct{}{}, &result)
	return result, err
}

func (c *Client) ListConditions(ctx context.Context, includeResolved bool, limit int) ([]bus.OperatorCondition, error) {
	var result []bus.OperatorCondition
	err := c.call(ctx, "list_conditions", map[string]interface{}{
		"include_resolved": includeResolved, "limit": limit,
	}, &result)
	return result, err
}

func (c *Client) AcknowledgeCondition(ctx context.Context, kind, subject string, generation int) (bus.OperatorCondition, error) {
	var result bus.OperatorCondition
	err := c.call(ctx, "acknowledge_condition", map[string]interface{}{
		"kind": kind, "subject": subject, "generation": generation,
	}, &result)
	return result, err
}

func (c *Client) SnoozeCondition(ctx context.Context, kind, subject string, generation int, until time.Time) (bus.OperatorCondition, error) {
	var result bus.OperatorCondition
	err := c.call(ctx, "snooze_condition", map[string]interface{}{
		"kind": kind, "subject": subject, "generation": generation, "until": until,
	}, &result)
	return result, err
}

func (c *Client) ClaimConditionPresentation(ctx context.Context, kind, subject string, generation int, lease time.Duration) (bool, error) {
	var result struct {
		Claimed bool `json:"claimed"`
	}
	err := c.call(ctx, "claim_condition_presentation", map[string]interface{}{
		"kind": kind, "subject": subject, "generation": generation, "lease_ns": int64(lease),
	}, &result)
	return result.Claimed, err
}

func (c *Client) InvokeReadCapability(ctx context.Context, name string, arguments json.RawMessage) (json.RawMessage, error) {
	return c.invokeCapability(ctx, "invoke_read_capability", name, arguments)
}

func (c *Client) InvokeWriteCapability(ctx context.Context, name string, arguments json.RawMessage) (json.RawMessage, error) {
	return c.invokeCapability(ctx, "invoke_write_capability", name, arguments)
}

func (c *Client) invokeCapability(ctx context.Context, operation, name string, arguments json.RawMessage) (json.RawMessage, error) {
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	invocation := bus.CapabilityInvocation{Name: name, Arguments: arguments}
	if err := bus.ValidateCapabilityInvocation(invocation); err != nil {
		return nil, err
	}
	var result json.RawMessage
	err := c.call(ctx, operation, invocation, &result)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	candidate := strings.TrimSpace(runID)
	if candidate != c.identity.RunID && candidate != c.helloIdentity.RunID {
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
	case "ping", "send", "check_inbox", "ack", "extend", "list_events", "who", "set_alias", "remove_alias", "list_aliases", "resolve_alias", "alias_preflight", "list_capabilities", "invoke_read_capability", "live_registrations", "monitor_attach", "expire_registration", "heartbeat_registrations", "list_conditions", "claim_condition_presentation", "archive_preflight":
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
	case errors.Is(err, bus.ErrAttentionUnavailable):
		code = "attention_unavailable"
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
	case errors.Is(err, bus.ErrActorArchived):
		code = "actor_archived"
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
	case errors.Is(err, bus.ErrRunNotLive):
		code = "run_not_live"
	case errors.Is(err, bus.ErrActorAdopted):
		code = "actor_adopted"
	case errors.Is(err, bus.ErrAliasConflict):
		code = "alias_conflict"
	case errors.Is(err, bus.ErrAliasNotFound):
		code = "alias_not_found"
	case errors.Is(err, bus.ErrAliasTombstoned):
		code = "alias_tombstoned"
	case errors.Is(err, bus.ErrAliasTargetUnknown):
		code = "alias_target_unknown"
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
	case "attention_unavailable":
		sentinel = bus.ErrAttentionUnavailable
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
	case "actor_archived":
		sentinel = bus.ErrActorArchived
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
	case "run_not_live":
		sentinel = bus.ErrRunNotLive
	case "actor_adopted":
		sentinel = bus.ErrActorAdopted
	case "alias_conflict":
		sentinel = bus.ErrAliasConflict
	case "alias_not_found":
		sentinel = bus.ErrAliasNotFound
	case "alias_tombstoned":
		sentinel = bus.ErrAliasTombstoned
	case "alias_target_unknown":
		sentinel = bus.ErrAliasTargetUnknown
	}
	if sentinel != nil {
		text := sentinel.Error()
		if rpc.Message == "" || rpc.Message == text {
			return sentinel
		}
		if strings.HasSuffix(rpc.Message, ": "+text) {
			return fmt.Errorf("%s: %w", strings.TrimSuffix(rpc.Message, ": "+text), sentinel)
		}
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
