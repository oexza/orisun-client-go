package orisun

import (
	"context"
	"net"
	"testing"

	eventstore "github.com/oexza/orisun-client-go/eventstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClient_GetServerInfo_RoundTrip(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	eventstore.RegisterEventStoreServer(server, serverInfoTestServer{})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := &OrisunClient{
		conn:   conn,
		client: eventstore.NewEventStoreClient(conn),
		logger: NewDefaultLogger(ERROR),
	}
	info, err := client.GetServerInfo(t.Context())
	require.NoError(t, err)
	require.Equal(t, "0.10.0", info.Version)
	require.Equal(t, eventstore.StorageBackend_STORAGE_BACKEND_SQLITE, info.Backend)
	require.Equal(t, "node-1", info.NodeId)
	require.Equal(t, []eventstore.ServerCapability{
		eventstore.ServerCapability_SERVER_CAPABILITY_COMMAND_CONTEXT_CONSISTENCY,
		eventstore.ServerCapability_SERVER_CAPABILITY_GRPC_HEALTH,
	}, info.Capabilities)
}

type serverInfoTestServer struct {
	eventstore.UnimplementedEventStoreServer
}

func (serverInfoTestServer) GetServerInfo(
	context.Context,
	*eventstore.GetServerInfoRequest,
) (*eventstore.GetServerInfoResponse, error) {
	return &eventstore.GetServerInfoResponse{
		Version:   "0.10.0",
		GitCommit: "abc123",
		BuildTime: "2026-07-25T12:00:00Z",
		Backend:   eventstore.StorageBackend_STORAGE_BACKEND_SQLITE,
		NodeId:    "node-1",
		Capabilities: []eventstore.ServerCapability{
			eventstore.ServerCapability_SERVER_CAPABILITY_COMMAND_CONTEXT_CONSISTENCY,
			eventstore.ServerCapability_SERVER_CAPABILITY_GRPC_HEALTH,
		},
	}, nil
}
