package wasmhost

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAPIKeyEnablesTLSByDefault(t *testing.T) {
	if got := grpcTransportCredentials("localhost:7233", GRPCOptions{}).Info().SecurityProtocol; got != "insecure" {
		t.Fatalf("default security protocol = %q, want insecure", got)
	}
	if got := grpcTransportCredentials("example.tmprl.cloud:7233", GRPCOptions{APIKey: "secret"}).Info().SecurityProtocol; got != "tls" {
		t.Fatalf("API-key security protocol = %q, want tls", got)
	}
	config := defaultTLSConfig("example.tmprl.cloud:7233")
	if config.ServerName != "example.tmprl.cloud" || config.MinVersion != tls.VersionTLS12 {
		t.Fatalf("default TLS config server name/min version = %q/%x", config.ServerName, config.MinVersion)
	}
}

func TestRawGRPCHostUsesTLSAndAPIKey(t *testing.T) {
	serverTLS, clientTLS := newTestTLSConfigs(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	authorization := make(chan []string, 1)
	server.RegisterService(&testRawGRPCServiceDesc, testRawGRPCService{handle: func(ctx context.Context) error {
		authorization <- metadata.ValueFromIncomingContext(ctx, "authorization")
		return nil
	}})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	host, err := NewRawGRPCHostWithOptions(listener.Addr().String(), GRPCOptions{
		TLS:    clientTLS,
		APIKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })

	response := host.CallContext(context.Background(), testGRPCRequest(metadata.Pairs("authorization", "Bearer wrong-key")))
	if response.Code != codes.OK {
		t.Fatalf("CallContext() code = %v, want OK (response %q)", response.Code, response.Proto)
	}
	select {
	case got := <-authorization:
		if len(got) != 1 || got[0] != "Bearer secret" {
			t.Fatalf("authorization metadata = %q, want API-key bearer token", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TLS request")
	}
}

func newTestTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		t.Fatal(err)
	}
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(certificate)
	return &tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{certificateDER}, PrivateKey: key}},
		}, &tls.Config{
			RootCAs:    rootCAs,
			ServerName: "localhost",
		}
}

func TestPrepareRequestMetadataExtractsGRPCTimeout(t *testing.T) {
	prepared, timeout, err := prepareRequestMetadata(metadata.Pairs(
		"grpc-timeout", "60000000u",
		"temporal-namespace", "default",
	))
	if err != nil {
		t.Fatal(err)
	}
	if timeout != time.Minute {
		t.Fatalf("timeout is %v, want one minute", timeout)
	}
	if got := prepared.Get("temporal-namespace"); len(got) != 1 || got[0] != "default" {
		t.Fatalf("namespace metadata is %v", got)
	}
}

func TestRawGRPCHostSendsRawBinaryRequestMetadata(t *testing.T) {
	want := string([]byte{0, 1, 2, 0xff})
	host := newTestRawGRPCHost(t, func(ctx context.Context) error {
		if got := metadata.ValueFromIncomingContext(ctx, "request-bin"); len(got) != 1 || got[0] != want {
			return status.Errorf(codes.Internal, "binary request metadata = %q", got)
		}
		return nil
	})

	response := host.CallContext(context.Background(), testGRPCRequest(metadata.Pairs("request-bin", want)))
	if response.Code != codes.OK {
		t.Fatalf("CallContext() code = %v, want OK (response %q)", response.Code, response.Message)
	}
}

func TestRawCodecMarshalReturnsOwnedSlice(t *testing.T) {
	codec := rawCodec{}
	payload := []byte("payload")
	message := rawMessage(payload)

	got, err := codec.Marshal(&message)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if !sameBackingBytes(got, payload) {
		t.Fatal("Marshal() did not return the message slice directly")
	}
}

func TestRawCodecUnmarshalKeepsProvidedSlice(t *testing.T) {
	codec := rawCodec{}
	payload := []byte("payload")
	var message rawMessage

	if err := codec.Unmarshal(payload, &message); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !sameBackingBytes([]byte(message), payload) {
		t.Fatal("Unmarshal() did not keep the provided slice")
	}
}

func TestRawCodecRejectsWrongTypes(t *testing.T) {
	codec := rawCodec{}
	if _, err := codec.Marshal([]byte("payload")); err == nil {
		t.Fatal("Marshal() accepted a non-*rawMessage value")
	}
	var target []byte
	if err := codec.Unmarshal([]byte("payload"), &target); err == nil {
		t.Fatal("Unmarshal() accepted a non-*rawMessage target")
	}
}

func TestRawGRPCHostPreservesResponseMetadata(t *testing.T) {
	headerBinary := string([]byte{0, 1, 2, 0xff})
	trailerBinary := string([]byte{3, 4, 5, 0xfe})
	host := newTestRawGRPCHost(t, func(ctx context.Context) error {
		if err := grpc.SetHeader(ctx, metadata.Pairs(
			"initial", "one",
			"initial-bin", headerBinary,
		)); err != nil {
			return err
		}
		grpc.SetTrailer(ctx, metadata.Pairs(
			"trailing", "two",
			"trailing-bin", trailerBinary,
		))
		return nil
	})

	response := host.CallContext(context.Background(), testGRPCRequest(nil))
	if response.Code != codes.OK {
		t.Fatalf("CallContext() code = %v, want OK", response.Code)
	}
	if got := response.Header.Get("initial"); len(got) != 1 || got[0] != "one" {
		t.Fatalf("initial metadata = %q, want one", got)
	}
	if got := response.Header.Get("initial-bin"); len(got) != 1 || got[0] != headerBinary {
		t.Fatalf("initial binary metadata = %q", got)
	}
	if got := response.Trailer.Get("trailing"); len(got) != 1 || got[0] != "two" {
		t.Fatalf("trailing metadata = %q, want two", got)
	}
	if got := response.Trailer.Get("trailing-bin"); len(got) != 1 || got[0] != trailerBinary {
		t.Fatalf("trailing binary metadata = %q", got)
	}
}

func TestRawGRPCHostPreservesStructuredStatusDetails(t *testing.T) {
	wantStatus, err := status.New(codes.ResourceExhausted, "limited").WithDetails(&emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	wantDetails, err := proto.Marshal(wantStatus.Proto())
	if err != nil {
		t.Fatal(err)
	}
	host := newTestRawGRPCHost(t, func(ctx context.Context) error {
		if err := grpc.SetHeader(ctx, metadata.Pairs("initial", "one")); err != nil {
			return err
		}
		grpc.SetTrailer(ctx, metadata.Pairs("trailing", "two"))
		return wantStatus.Err()
	})

	response := host.CallContext(context.Background(), testGRPCRequest(nil))
	if response.Code != wantStatus.Code() || response.Message != wantStatus.Message() {
		t.Fatalf("status = (%v, %q), want (%v, %q)", response.Code, response.Message, wantStatus.Code(), wantStatus.Message())
	}
	if !bytes.Equal(response.Details, wantDetails) {
		t.Fatalf("status details = %v, want %v", response.Details, wantDetails)
	}
	if got := response.Header.Get("initial"); len(got) != 1 || got[0] != "one" {
		t.Fatalf("initial metadata = %q, want one", got)
	}
	if got := response.Trailer.Get("trailing"); len(got) != 1 || got[0] != "two" {
		t.Fatalf("trailing metadata = %q, want two", got)
	}
	if response.Trailer.Get("grpc-status-details-bin") != nil {
		t.Fatal("structured details were duplicated in trailing metadata")
	}
}

func sameBackingBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	return &a[0] == &b[0]
}

func TestRawGRPCHostCallContextPreCancelled(t *testing.T) {
	host := newTestRawGRPCHost(t, func(context.Context) error {
		t.Fatal("server handler should not run for a pre-cancelled call")
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	response := host.CallContext(ctx, testGRPCRequest(nil))
	if response.Code != codes.Canceled {
		t.Fatalf("CallContext() code = %v, want %v (response %q)", response.Code, codes.Canceled, response.Message)
	}
	if !strings.Contains(response.Message, context.Canceled.Error()) {
		t.Fatalf("CallContext() response = %q, want context cancellation", response.Message)
	}
}

func TestRawGRPCHostTimingHookRecordsElapsedCall(t *testing.T) {
	recorded := make(chan time.Duration, 1)
	restore := SetTimingHook(func(method string, elapsed time.Duration) {
		if method == "/test.RawGRPC/Unary" {
			recorded <- elapsed
		}
	})
	defer restore()

	host := newTestRawGRPCHost(t, func(context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	response := host.CallContext(context.Background(), testGRPCRequest(nil))
	if response.Code != codes.OK {
		t.Fatalf("CallContext() code = %v, want OK (response %q)", response.Code, response.Proto)
	}
	select {
	case elapsed := <-recorded:
		if elapsed < 5*time.Millisecond {
			t.Fatalf("recorded call = %v, want a real elapsed duration", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("timing hook was not called")
	}
}

func TestRawGRPCHostCloseCancelsInflightCall(t *testing.T) {
	started := make(chan struct{})
	host := newTestRawGRPCHost(t, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return status.Error(codes.Canceled, ctx.Err().Error())
	})

	type callResult struct {
		response GRPCResponse
	}
	resultCh := make(chan callResult, 1)
	go func() {
		resultCh <- callResult{response: host.CallContext(context.Background(), testGRPCRequest(nil))}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server handler to start")
	}

	if err := host.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case result := <-resultCh:
		if result.response.Code != codes.Canceled {
			t.Fatalf("CallContext() code after Close() = %v, want %v (response %q)", result.response.Code, codes.Canceled, result.response.Message)
		}
		if !strings.Contains(result.response.Message, context.Canceled.Error()) {
			t.Fatalf("CallContext() response after Close() = %q, want context cancellation", result.response.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for in-flight call to finish after Close()")
	}
}

type testRawGRPCServer interface {
	Unary(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type testRawGRPCService struct {
	handle func(context.Context) error
}

func (s testRawGRPCService) Unary(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if s.handle != nil {
		if err := s.handle(ctx); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

var testRawGRPCServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.RawGRPC",
	HandlerType: (*testRawGRPCServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Unary",
			Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				return srv.(testRawGRPCServer).Unary(ctx, in)
			},
		},
	},
}

func testGRPCRequest(md metadata.MD) GRPCRequest {
	return GRPCRequest{Service: "test.RawGRPC", RPC: "Unary", Metadata: md}
}

func newTestRawGRPCHost(t *testing.T, handle func(context.Context) error) *RawGRPCHost {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	server.RegisterService(&testRawGRPCServiceDesc, testRawGRPCService{handle: handle})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}

	hostCtx, cancel := context.WithCancel(context.Background())
	host := &RawGRPCHost{conn: conn, ctx: hostCtx, cancel: cancel}
	t.Cleanup(func() {
		_ = host.Close()
	})
	return host
}
