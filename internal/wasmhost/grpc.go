package wasmhost

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type RawGRPCHost struct {
	conn          *grpc.ClientConn
	ctx           context.Context
	cancel        context.CancelFunc
	authorization string
	mu            sync.Mutex
}

type timingHookFunc func(method string, elapsed time.Duration)

var timingHook atomic.Pointer[timingHookFunc]

const temporalServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// SetTimingHook installs an optional gRPC timing hook and returns a restore function.
func SetTimingHook(hook func(method string, elapsed time.Duration)) func() {
	var installed *timingHookFunc
	if hook != nil {
		typedHook := timingHookFunc(hook)
		installed = &typedHook
	}
	previous := timingHook.Swap(installed)
	return func() {
		timingHook.CompareAndSwap(installed, previous)
	}
}

func recordTiming(method string, elapsed time.Duration) {
	hook := timingHook.Load()
	if hook == nil || *hook == nil {
		return
	}
	(*hook)(method, elapsed)
}

func timingEnabled() bool {
	return timingHook.Load() != nil
}

type GRPCOptions struct {
	TLS    *tls.Config
	APIKey string
}

// GRPCResponse retains the complete unary response needed to reconstruct the
// callback transport response inside Core.
type GRPCResponse struct {
	Code    codes.Code
	Message string
	Details []byte
	Header  metadata.MD
	Trailer metadata.MD
	Proto   []byte
}

// GRPCRequest is one unary call received from Temporal Core.
type GRPCRequest struct {
	Service  string
	RPC      string
	Metadata metadata.MD
	Proto    []byte
}

func NewRawGRPCHost(address string) (*RawGRPCHost, error) {
	return NewRawGRPCHostWithOptions(address, GRPCOptions{})
}

func NewRawGRPCHostWithOptions(address string, options GRPCOptions) (*RawGRPCHost, error) {
	transportCredentials := grpcTransportCredentials(address, options)
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultServiceConfig(temporalServiceConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	authorization := ""
	if options.APIKey != "" {
		authorization = "Bearer " + options.APIKey
	}
	return &RawGRPCHost{
		conn:          conn,
		ctx:           ctx,
		cancel:        cancel,
		authorization: authorization,
	}, nil
}

func grpcTransportCredentials(address string, options GRPCOptions) credentials.TransportCredentials {
	if options.TLS != nil {
		return credentials.NewTLS(options.TLS.Clone())
	}
	if options.APIKey != "" {
		return credentials.NewTLS(defaultTLSConfig(address))
	}
	return insecure.NewCredentials()
}

func defaultTLSConfig(address string) *tls.Config {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if host, _, err := net.SplitHostPort(address); err == nil {
		config.ServerName = host
	}
	return config
}

func (h *RawGRPCHost) Close() error {
	h.mu.Lock()
	if h.conn == nil {
		h.mu.Unlock()
		return nil
	}
	conn := h.conn
	cancel := h.cancel
	h.conn = nil
	h.ctx = nil
	h.cancel = nil
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return conn.Close()
}

func (h *RawGRPCHost) Closed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conn == nil
}

func (h *RawGRPCHost) Call(request GRPCRequest) GRPCResponse {
	return h.CallContext(context.Background(), request)
}

func (h *RawGRPCHost) CallContext(ctx context.Context, request GRPCRequest) GRPCResponse {
	headers, timeout, err := prepareRequestMetadata(request.Metadata)
	if err != nil {
		return grpcErrorResponse(status.New(codes.Internal, err.Error()), nil, nil)
	}
	h.mu.Lock()
	conn := h.conn
	hostCtx := h.ctx
	authorization := h.authorization
	h.mu.Unlock()
	if conn == nil || hostCtx == nil {
		return grpcErrorResponse(status.New(codes.Unavailable, "gRPC host is closed"), nil, nil)
	}
	if authorization != "" {
		headers.Set("authorization", authorization)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(hostCtx, cancel)
	defer stop()
	ctx = metadata.NewOutgoingContext(ctx, headers)
	// grpc v1.83 calls the codec synchronously during Invoke, so the request
	// bytes stay owned and immutable for the full marshal path.
	input := rawMessage(request.Proto)
	var output rawMessage
	var responseHeader metadata.MD
	var responseTrailer metadata.MD
	method := "/" + request.Service + "/" + request.RPC
	var start time.Time
	if timingEnabled() {
		start = time.Now()
	}
	if os.Getenv("TEMPORAL_CORE_WASM_TRACE") != "" {
		fmt.Fprintf(os.Stderr, "Temporal Core gRPC: %s (timeout %v)\n", method, timeout)
		if request.RPC == "PollActivityTaskQueue" {
			var poll workflowservicepb.PollActivityTaskQueueRequest
			if err := proto.Unmarshal(request.Proto, &poll); err == nil {
				fmt.Fprintf(
					os.Stderr,
					"Temporal Core activity poll: namespace=%q task_queue=%q identity=%q\n",
					poll.Namespace,
					poll.GetTaskQueue().GetName(),
					poll.Identity,
				)
			}
		}
	}
	if err := conn.Invoke(
		ctx,
		method,
		&input,
		&output,
		grpc.ForceCodec(rawCodec{}),
		grpc.Header(&responseHeader),
		grpc.Trailer(&responseTrailer),
	); err != nil {
		if !start.IsZero() {
			recordTiming(method, time.Since(start))
		}
		if os.Getenv("TEMPORAL_CORE_WASM_TRACE") != "" {
			fmt.Fprintf(os.Stderr, "Temporal Core gRPC error: %v\n", err)
		}
		return grpcErrorResponse(status.Convert(err), responseHeader, responseTrailer)
	}
	if !start.IsZero() {
		recordTiming(method, time.Since(start))
	}
	return GRPCResponse{
		Code:    codes.OK,
		Header:  responseHeader,
		Trailer: responseTrailer,
		Proto:   output,
	}
}

func grpcErrorResponse(responseStatus *status.Status, header, trailer metadata.MD) GRPCResponse {
	trailer = trailer.Copy()
	response := GRPCResponse{
		Code:    responseStatus.Code(),
		Message: responseStatus.Message(),
		Header:  header,
		Trailer: trailer,
	}
	if values := trailer.Get("grpc-status-details-bin"); len(values) == 1 {
		response.Details = []byte(values[0])
	} else if statusProto := responseStatus.Proto(); len(statusProto.GetDetails()) != 0 {
		response.Details, _ = proto.Marshal(statusProto)
	}
	trailer.Delete("grpc-status")
	trailer.Delete("grpc-message")
	trailer.Delete("grpc-status-details-bin")
	return response
}

func prepareRequestMetadata(input metadata.MD) (metadata.MD, time.Duration, error) {
	md := metadata.MD{}
	var timeout time.Duration
	for inputName, values := range input {
		name := strings.ToLower(inputName)
		for _, value := range values {
			if name == "grpc-timeout" {
				var err error
				timeout, err = parseGRPCTimeout(value)
				if err != nil {
					return nil, 0, err
				}
				continue
			}
			if isTransportHeader(name) {
				continue
			}
			md.Append(name, value)
		}
	}
	return md, timeout, nil
}

func parseGRPCTimeout(value string) (time.Duration, error) {
	if len(value) < 2 {
		return 0, fmt.Errorf("invalid grpc-timeout %q", value)
	}
	amount, err := strconv.ParseUint(value[:len(value)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid grpc-timeout %q: %w", value, err)
	}
	units := map[byte]time.Duration{
		'H': time.Hour,
		'M': time.Minute,
		'S': time.Second,
		'm': time.Millisecond,
		'u': time.Microsecond,
		'n': time.Nanosecond,
	}
	unit, ok := units[value[len(value)-1]]
	if !ok {
		return 0, fmt.Errorf("invalid grpc-timeout unit in %q", value)
	}
	return time.Duration(amount) * unit, nil
}

func isTransportHeader(name string) bool {
	return strings.HasPrefix(name, ":") || name == "content-type" || name == "te" ||
		name == "user-agent" || name == "grpc-encoding" || name == "grpc-accept-encoding" ||
		name == "grpc-timeout"
}

type rawMessage []byte

type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(value any) ([]byte, error) {
	message, ok := value.(*rawMessage)
	if !ok {
		return nil, fmt.Errorf("raw codec cannot marshal %T", value)
	}
	return *message, nil
}

func (rawCodec) Unmarshal(data []byte, value any) error {
	message, ok := value.(*rawMessage)
	if !ok {
		return fmt.Errorf("raw codec cannot unmarshal %T", value)
	}
	// grpc's codecV0Bridge.Unmarshal already Materialize()s into owned storage
	// before delegating here, so we can keep the provided slice directly.
	*message = data
	return nil
}
