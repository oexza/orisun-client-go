package orisun

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	eventstore "github.com/oexza/orisun-client-go/eventstore"
)

const (
	defaultMaxReceiveMessageSize = 100 * 1024 * 1024
	defaultMaxSendMessageSize    = 100 * 1024 * 1024
	defaultFlowControlWindow     = 1024 * 1024
	maxInt32Value                = 1<<31 - 1
)

// Client is the primary Orisun client type.
type Client = OrisunClient

// OrisunClient is the main client for interacting with the Orisun event store
type OrisunClient struct {
	conn           *grpc.ClientConn
	client         eventstore.EventStoreClient
	adminClient    eventstore.AdminClient
	defaultTimeout time.Duration
	logger         Logger
	tokenCache     *TokenCache
	username       string
	password       string
	mu             sync.RWMutex
	closed         bool
	ownsConn       bool
}

// ClientBuilder is used to build an OrisunClient instance
type ClientBuilder struct {
	servers                     []*ServerAddress
	timeoutSeconds              int
	useTLS                      bool
	loadBalancingPolicy         string
	username                    string
	password                    string
	logger                      Logger
	logLevel                    LogLevel
	useDnsResolver              bool
	keepAliveTimeMs             time.Duration
	keepAliveTimeoutMs          time.Duration
	keepAlivePermitWithoutCalls bool
	dnsTarget                   string
	staticTarget                string
	target                      string
	channel                     *grpc.ClientConn
	transportCredentials        credentials.TransportCredentials
	dialOptions                 []grpc.DialOption
	tokenCache                  *TokenCache
	maxReceiveMessageSize       int
	maxSendMessageSize          int
	flowControlWindow           int
}

// NewClientBuilder creates a new ClientBuilder with default values
func NewClientBuilder() *ClientBuilder {
	return &ClientBuilder{
		timeoutSeconds:              30,
		useTLS:                      false,
		loadBalancingPolicy:         "round_robin",
		logLevel:                    INFO,
		useDnsResolver:              true,
		keepAliveTimeMs:             30 * time.Second,
		keepAliveTimeoutMs:          10 * time.Second,
		keepAlivePermitWithoutCalls: true,
		maxReceiveMessageSize:       defaultMaxReceiveMessageSize,
		maxSendMessageSize:          defaultMaxSendMessageSize,
		flowControlWindow:           defaultFlowControlWindow,
		servers:                     make([]*ServerAddress, 0),
	}
}

// WithHost adds a server with the given host and default port
func (b *ClientBuilder) WithHost(host string) *ClientBuilder {
	return b.WithServer(host, 5005)
}

// WithPort sets the port for the last added server
func (b *ClientBuilder) WithPort(port int) *ClientBuilder {
	if len(b.servers) == 0 {
		b.servers = append(b.servers, NewServerAddress("localhost", port))
	} else {
		lastServer := b.servers[len(b.servers)-1]
		b.servers[len(b.servers)-1] = NewServerAddress(lastServer.Host, port)
	}
	return b
}

// WithServer adds a server with the given host and port
func (b *ClientBuilder) WithServer(host string, port int) *ClientBuilder {
	b.servers = append(b.servers, NewServerAddress(host, port))
	return b
}

// WithServers adds multiple servers
func (b *ClientBuilder) WithServers(servers []*ServerAddress) *ClientBuilder {
	b.servers = append(b.servers, servers...)
	return b
}

// WithLoadBalancingPolicy sets the load balancing policy
func (b *ClientBuilder) WithLoadBalancingPolicy(policy string) *ClientBuilder {
	b.loadBalancingPolicy = policy
	return b
}

// WithDnsTarget sets the DNS target for DNS-based load balancing
func (b *ClientBuilder) WithDnsTarget(dnsTarget string) *ClientBuilder {
	b.dnsTarget = dnsTarget
	return b
}

// WithStaticTarget sets the static target for static-based load balancing
func (b *ClientBuilder) WithStaticTarget(staticTarget string) *ClientBuilder {
	b.staticTarget = staticTarget
	return b
}

// WithTarget sets a raw gRPC target, for example "localhost:5005" or "dns:///orisun:5005".
func (b *ClientBuilder) WithTarget(target string) *ClientBuilder {
	b.target = target
	return b
}

// WithDnsResolver sets whether to use DNS resolver
func (b *ClientBuilder) WithDnsResolver(useDns bool) *ClientBuilder {
	b.useDnsResolver = useDns
	return b
}

// WithTimeout sets the default timeout in seconds
func (b *ClientBuilder) WithTimeout(seconds int) *ClientBuilder {
	b.timeoutSeconds = seconds
	return b
}

func (b *ClientBuilder) defaultTimeout(timeout time.Duration) *ClientBuilder {
	if timeout <= 0 {
		return b
	}
	seconds := int(timeout / time.Second)
	if timeout%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	b.timeoutSeconds = seconds
	return b
}

// WithTLS sets whether to use TLS
func (b *ClientBuilder) WithTLS(useTLS bool) *ClientBuilder {
	b.useTLS = useTLS
	return b
}

// WithTransportCredentials sets explicit gRPC transport credentials.
func (b *ClientBuilder) WithTransportCredentials(creds credentials.TransportCredentials) *ClientBuilder {
	b.transportCredentials = creds
	b.useTLS = creds != nil
	return b
}

// WithDialOptions appends custom gRPC dial options.
func (b *ClientBuilder) WithDialOptions(opts ...grpc.DialOption) *ClientBuilder {
	b.dialOptions = append(b.dialOptions, opts...)
	return b
}

// WithChannel sets a custom gRPC channel
func (b *ClientBuilder) WithChannel(channel *grpc.ClientConn) *ClientBuilder {
	b.channel = channel
	return b
}

// WithBasicAuth sets the basic authentication credentials
func (b *ClientBuilder) WithBasicAuth(username, password string) *ClientBuilder {
	b.username = username
	b.password = password
	return b
}

// WithLogger sets the logger
func (b *ClientBuilder) WithLogger(logger Logger) *ClientBuilder {
	b.logger = logger
	return b
}

// WithLogLevel sets the log level
func (b *ClientBuilder) WithLogLevel(level LogLevel) *ClientBuilder {
	b.logLevel = level
	return b
}

// WithKeepAliveTime sets the keep-alive time
func (b *ClientBuilder) WithKeepAliveTime(keepAliveTimeMs time.Duration) *ClientBuilder {
	b.keepAliveTimeMs = keepAliveTimeMs
	return b
}

// WithKeepAliveTimeout sets the keep-alive timeout
func (b *ClientBuilder) WithKeepAliveTimeout(keepAliveTimeoutMs time.Duration) *ClientBuilder {
	b.keepAliveTimeoutMs = keepAliveTimeoutMs
	return b
}

// WithKeepAlivePermitWithoutCalls sets whether to permit keep-alive without calls
func (b *ClientBuilder) WithKeepAlivePermitWithoutCalls(permitWithoutCalls bool) *ClientBuilder {
	b.keepAlivePermitWithoutCalls = permitWithoutCalls
	return b
}

// WithMaxReceiveMessageSize sets the maximum inbound gRPC message size in bytes.
func (b *ClientBuilder) WithMaxReceiveMessageSize(bytes int) *ClientBuilder {
	if bytes > 0 {
		b.maxReceiveMessageSize = bytes
	}
	return b
}

// WithMaxSendMessageSize sets the maximum outbound gRPC message size in bytes.
func (b *ClientBuilder) WithMaxSendMessageSize(bytes int) *ClientBuilder {
	if bytes > 0 {
		b.maxSendMessageSize = bytes
	}
	return b
}

// WithFlowControlWindow sets the HTTP/2 stream and connection flow-control window.
func (b *ClientBuilder) WithFlowControlWindow(bytes int) *ClientBuilder {
	if bytes > 0 && bytes <= maxInt32Value {
		b.flowControlWindow = bytes
	}
	return b
}

// Build creates the OrisunClient instance
func (b *ClientBuilder) Build() (*OrisunClient, error) {
	// Initialize logger
	var clientLogger Logger
	if b.logger == nil {
		clientLogger = NewDefaultLogger(b.logLevel)
		b.logger = clientLogger
	} else {
		clientLogger = b.logger
	}

	// Initialize token cache
	clientTokenCache := NewTokenCache(clientLogger)
	b.tokenCache = clientTokenCache

	var conn *grpc.ClientConn
	var err error
	ownsConn := false

	if b.channel != nil {
		conn = b.channel
	} else {
		// Create channel based on configuration
		if strings.TrimSpace(b.target) != "" {
			conn, err = b.createChannel(b.target)
		} else if b.dnsTarget != "" && strings.TrimSpace(b.dnsTarget) != "" {
			// DNS-based load balancing
			target := b.dnsTarget
			if !strings.HasPrefix(target, "dns:///") {
				target = "dns:///" + target
			}
			conn, err = b.createChannel(target)
		} else if b.staticTarget != "" && strings.TrimSpace(b.staticTarget) != "" {
			// Static-based load balancing
			target := b.staticTarget
			if !strings.HasPrefix(target, "static:///") {
				target = "static:///" + target
			}
			conn, err = b.createChannel(target)
		} else {
			// Traditional server-based load balancing
			if len(b.servers) == 0 {
				// Default to localhost if no servers specified
				b.servers = append(b.servers, NewServerAddress("localhost", 5005))
			}

			var target string
			if len(b.servers) == 1 {
				// Single server case
				server := b.servers[0]
				target = fmt.Sprintf("%s:%d", server.Host, server.Port)
			} else {
				// Multiple servers case - use name resolver and load balancing
				// Check if hosts contain commas for manual load balancing
				hasCommaSeparatedHosts := false
				for _, server := range b.servers {
					if strings.Contains(server.Host, ",") {
						hasCommaSeparatedHosts = true
						break
					}
				}

				if hasCommaSeparatedHosts {
					// Handle comma-separated list of hosts for manual load balancing
					var hosts []string
					for _, server := range b.servers {
						hosts = append(hosts, fmt.Sprintf("%s:%d", server.Host, server.Port))
					}
					target = strings.Join(hosts, ",")
				} else {
					// Use DNS or static resolver
					target = b.createTargetString(b.servers)
				}
			}

			conn, err = b.createChannel(target)
		}
		ownsConn = err == nil

		if err != nil {
			return nil, NewOrisunExceptionWithCause("Failed to create gRPC channel", err)
		}
	}

	client := &OrisunClient{
		conn:           conn,
		client:         eventstore.NewEventStoreClient(conn),
		adminClient:    eventstore.NewAdminClient(conn),
		defaultTimeout: time.Duration(b.timeoutSeconds) * time.Second,
		logger:         clientLogger,
		tokenCache:     clientTokenCache,
		username:       b.username,
		password:       b.password,
		closed:         false,
		ownsConn:       ownsConn,
	}

	clientLogger.Info("OrisunClient initialized with timeout: {} seconds", b.timeoutSeconds)
	return client, nil
}

// createChannel creates a gRPC channel with the given target
func (b *ClientBuilder) createChannel(target string) (*grpc.ClientConn, error) {
	kp := keepalive.ClientParameters{
		Time:                b.keepAliveTimeMs,
		Timeout:             b.keepAliveTimeoutMs,
		PermitWithoutStream: b.keepAlivePermitWithoutCalls,
	}

	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(kp),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(b.maxReceiveMessageSize),
			grpc.MaxCallSendMsgSize(b.maxSendMessageSize),
		),
		grpc.WithInitialWindowSize(int32(b.flowControlWindow)),
		grpc.WithInitialConnWindowSize(int32(b.flowControlWindow)),
	}

	// Set up load balancing configuration
	if b.loadBalancingPolicy != "" {
		serviceConfig := fmt.Sprintf(`{"loadBalancingConfig": [{"%s": {}}]}`, b.loadBalancingPolicy)
		opts = append(opts, grpc.WithDefaultServiceConfig(serviceConfig))
	}

	if !b.useTLS {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else if b.transportCredentials != nil {
		opts = append(opts, grpc.WithTransportCredentials(b.transportCredentials))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})))
	}

	// Add authentication interceptor if credentials are provided
	if b.username != "" && b.password != "" {
		basicAuth := CreateBasicAuthCredentials(b.username, b.password)
		opts = append(opts, grpc.WithUnaryInterceptor(b.createAuthInterceptor(basicAuth)))
		opts = append(opts, grpc.WithStreamInterceptor(b.createStreamAuthInterceptor(basicAuth)))
	}

	opts = append(opts, b.dialOptions...)

	return grpc.Dial(target, opts...)
}

// createAuthInterceptor creates a unary interceptor for authentication
func (b *ClientBuilder) createAuthInterceptor(basicAuth string) grpc.UnaryClientInterceptor {
	tokenCache := b.tokenCache
	if tokenCache == nil {
		tokenCache = NewTokenCache(b.logger)
	}

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Add authentication metadata
		md := tokenCache.CreateAuthMetadata(basicAuth)
		if existing, ok := metadata.FromOutgoingContext(ctx); ok {
			md = metadata.Join(existing, md)
		}
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Create a trailer-only context to extract response headers
		var responseMD metadata.MD

		// Add options to capture response headers and trailers
		opts = append(opts, grpc.Header(&responseMD))
		// Call the invoker with a custom option to capture trailers
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Extract and cache token from response metadata
		if responseMD != nil {
			b.logger.Debug("Extracting token from response metadata")
			tokenCache.ExtractAndCacheToken(responseMD)
		}

		return err
	}
}

// createStreamAuthInterceptor creates a stream interceptor for authentication
func (b *ClientBuilder) createStreamAuthInterceptor(basicAuth string) grpc.StreamClientInterceptor {
	tokenCache := b.tokenCache
	if tokenCache == nil {
		tokenCache = NewTokenCache(b.logger)
	}

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Add authentication metadata
		md := tokenCache.CreateAuthMetadata(basicAuth)
		if existing, ok := metadata.FromOutgoingContext(ctx); ok {
			md = metadata.Join(existing, md)
		}
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Create the stream with trailer capture
		stream, err := streamer(ctx, desc, cc, method, opts...)

		// Set up a function to extract tokens from trailers when available
		if stream != nil {
			// Note: In gRPC-Go, trailer extraction is handled differently
			// We'll need to use stream-specific methods to access trailers
			// This is a simplified approach - in production, you might need
			// to implement custom stream wrapping to properly intercept trailers
		}

		return stream, err
	}
}

// createTargetString creates a target string from server addresses
func (b *ClientBuilder) createTargetString(servers []*ServerAddress) string {
	var sb strings.Builder
	prefix := "dns:///"
	if !b.useDnsResolver {
		prefix = "static:///"
	}
	sb.WriteString(prefix)

	for i, server := range servers {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%s:%d", server.Host, server.Port))
	}

	return sb.String()
}

// Close closes the client connection
func (c *OrisunClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		c.logger.Debug("OrisunClient already closed")
		return nil
	}

	c.logger.Debug("Closing OrisunClient connection")

	if c.conn != nil && c.ownsConn {
		err := c.conn.Close()
		if err != nil {
			c.logger.Error("Error closing connection: {}", err)
			return err
		}
		c.logger.Info("OrisunClient connection closed successfully")
	}

	c.closed = true
	return nil
}

// IsClosed returns true if the client is closed
func (c *OrisunClient) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// GetDefaultTimeout returns the default timeout for the client
func (c *OrisunClient) GetDefaultTimeout() time.Duration {
	return c.defaultTimeout
}

// GetLogger returns the logger used by the client
func (c *OrisunClient) GetLogger() Logger {
	return c.logger
}

// Ping pings the server to check connectivity
func (c *OrisunClient) Ping(ctx context.Context) error {
	c.logger.Debug("Pinging server")

	request := &eventstore.PingRequest{}
	_, err := c.client.Ping(ctx, request)

	if err != nil {
		return err
	}

	c.logger.Debug("Ping successful")
	return nil
}

// GetServerInfo returns build, backend, node identity, and capability
// information for the server that handles this call.
func (c *OrisunClient) GetServerInfo(ctx context.Context) (*eventstore.GetServerInfoResponse, error) {
	c.logger.Debug("Getting server information")

	response, err := c.client.GetServerInfo(ctx, &eventstore.GetServerInfoRequest{})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// HealthCheck performs a health check
func (c *OrisunClient) HealthCheck(ctx context.Context, boundary string) (bool, error) {
	c.logger.Debug("Performing health check")

	// Try to ping the server
	if err := c.Ping(ctx); err != nil {
		return false, NewOrisunExceptionWithCause("Health check failed - ping failed", err).
			AddContext("operation", "healthCheck")
	}

	// Try to make a simple call to test connectivity
	request := &eventstore.GetEventsRequest{
		Boundary: boundary,
		Count:    1,
	}
	_, err := c.GetEvents(ctx, request)
	if err != nil {
		return false, NewOrisunExceptionWithCause("Health check failed - get events failed", err).
			AddContext("operation", "healthCheck").
			AddContext("boundary", boundary)
	}

	c.logger.Debug("Health check successful")
	return true, nil
}

// SaveEvents saves events to a boundary.
//
// Deprecated: use SaveEventsV2.
func (c *OrisunClient) SaveEvents(ctx context.Context, request *eventstore.SaveEventsRequest) (*eventstore.WriteResult, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateSaveEventsRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Saving {} in boundary '{}'",
		len(request.Events), request.Boundary)

	// Make the gRPC call
	response, err := c.client.SaveEvents(ctx, request)
	if err != nil {
		return nil, c.handleSaveException(err, "saveEvents")
	}

	c.logger.Info("Successfully saved {} events ",
		len(request.Events))

	return response, nil
}

// SaveEventsV2 saves events after atomically validating every consistency observation.
func (c *OrisunClient) SaveEventsV2(ctx context.Context, request *eventstore.SaveEventsV2Request) (*eventstore.WriteResult, error) {
	validator := NewRequestValidator()
	if err := validator.ValidateSaveEventsV2Request(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Saving {} events in boundary '{}' with {} consistency observations",
		len(request.Events), request.Boundary, len(request.Consistency))

	response, err := c.client.SaveEventsV2(ctx, request)
	if err != nil {
		return nil, c.handleSaveException(err, "saveEventsV2")
	}

	c.logger.Info("Successfully saved {} events", len(request.Events))
	return response, nil
}

// GetEvents retrieves events from the event store
func (c *OrisunClient) GetEvents(ctx context.Context, request *eventstore.GetEventsRequest) (*eventstore.GetEventsResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateGetEventsRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Getting events from boundary: {}", request.Boundary)

	// Make the gRPC call
	response, err := c.client.GetEvents(ctx, request)
	if err != nil {
		return nil, c.handleGetException(err, request)
	}

	c.logger.Debug("Successfully retrieved {} events", len(response.Events))
	return response, nil
}

// GetLatestByCriteria retrieves the latest event for each criterion from one server-side read snapshot.
func (c *OrisunClient) GetLatestByCriteria(ctx context.Context, request *eventstore.GetLatestByCriteriaRequest) (*eventstore.GetLatestByCriteriaResponse, error) {
	validator := NewRequestValidator()
	if err := validator.ValidateGetLatestByCriteriaRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Getting latest events by criteria from boundary: {}", request.Boundary)

	response, err := c.client.GetLatestByCriteria(ctx, request)
	if err != nil {
		return nil, c.handleGetLatestByCriteriaException(err, request)
	}

	c.logger.Debug("Successfully retrieved {} latest criteria results", len(response.Results))
	return response, nil
}

// SubscribeToEvents subscribes to events from the event store
func (c *OrisunClient) SubscribeToEvents(ctx context.Context, request *eventstore.CatchUpSubscribeToEventStoreRequest, handler EventHandler) (*EventSubscription, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateSubscribeRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Subscribing to events in boundary '{}' with subscriber '{}'",
		request.Boundary, request.SubscriberName)

	// Create cancel function for the subscription
	ctx, cancel := context.WithCancel(ctx)

	// Make the gRPC streaming call
	stream, err := c.client.CatchUpSubscribeToEvents(ctx, request)
	if err != nil {
		cancel()
		return nil, c.handleSubscribeException(err, request)
	}

	// Create and return subscription
	subscription := NewEventSubscription(stream, handler, c.logger, cancel)
	return subscription, nil
}

// handleSaveException handles exceptions from SaveEvents operations
func (c *OrisunClient) handleSaveException(err error, operation string) error {
	st, ok := status.FromError(err)
	if !ok {
		return NewOrisunExceptionWithCause("Failed to save events", err).
			AddContext("operation", operation)
	}

	if st.Code() == codes.AlreadyExists {
		// Extract version numbers from error description
		expected, actual, parseErr := ExtractVersionNumbers(st.Message())
		if parseErr == nil {
			return NewOptimisticConcurrencyExceptionWithCause(
				st.Message(), expected, actual, err).
				AddContext("operation", operation).
				AddContext("expectedVersion", expected).
				AddContext("actualVersion", actual)
		}
	}

	return NewOrisunExceptionWithCause("Failed to save events", err).
		AddContext("operation", operation).
		AddContext("statusCode", st.Code().String()).
		AddContext("statusDescription", st.Message())
}

// handleGetException handles exceptions from GetEvents operations
func (c *OrisunClient) handleGetException(err error, request *eventstore.GetEventsRequest) error {
	st, ok := status.FromError(err)
	if !ok {
		return NewOrisunExceptionWithCause("Failed to get events", err).
			AddContext("operation", "getEvents").
			AddContext("boundary", request.Boundary)
	}

	return NewOrisunExceptionWithCause("Failed to get events", err).
		AddContext("operation", "getEvents").
		AddContext("boundary", request.Boundary).
		AddContext("statusCode", st.Code().String()).
		AddContext("statusDescription", st.Message())
}

// handleGetLatestByCriteriaException handles exceptions from GetLatestByCriteria operations
func (c *OrisunClient) handleGetLatestByCriteriaException(err error, request *eventstore.GetLatestByCriteriaRequest) error {
	st, ok := status.FromError(err)
	if !ok {
		return NewOrisunExceptionWithCause("Failed to get latest events by criteria", err).
			AddContext("operation", "getLatestByCriteria").
			AddContext("boundary", request.Boundary)
	}

	return NewOrisunExceptionWithCause("Failed to get latest events by criteria", err).
		AddContext("operation", "getLatestByCriteria").
		AddContext("boundary", request.Boundary).
		AddContext("statusCode", st.Code().String()).
		AddContext("statusDescription", st.Message())
}

// handleSubscribeException handles exceptions from subscription operations
func (c *OrisunClient) handleSubscribeException(err error, request any) error {
	st, ok := status.FromError(err)
	if !ok {
		return NewOrisunExceptionWithCause("Failed to create subscription", err).
			AddContext("operation", "subscribe")
	}

	return NewOrisunExceptionWithCause("Failed to create subscription", err).
		AddContext("operation", "subscribe").
		AddContext("statusCode", st.Code().String()).
		AddContext("statusDescription", st.Message())
}

// CreateBoundary records a catalog definition and idempotently provisions its
// physical storage.
func (c *OrisunClient) CreateBoundary(ctx context.Context, request *eventstore.CreateBoundaryRequest) (*eventstore.CreateBoundaryResponse, error) {
	validator := NewRequestValidator()
	if err := validator.ValidateCreateBoundaryRequest(request); err != nil {
		return nil, err
	}

	response, err := c.adminClient.CreateBoundary(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "createBoundary")
	}
	return response, nil
}

// ListBoundaries returns the event-backed boundary catalog.
func (c *OrisunClient) ListBoundaries(ctx context.Context, request *eventstore.ListBoundariesRequest) (*eventstore.ListBoundariesResponse, error) {
	validator := NewRequestValidator()
	if err := validator.ValidateListBoundariesRequest(request); err != nil {
		return nil, err
	}

	response, err := c.adminClient.ListBoundaries(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "listBoundaries")
	}
	return response, nil
}

// GetBoundary returns one boundary from the event-backed catalog.
func (c *OrisunClient) GetBoundary(ctx context.Context, request *eventstore.GetBoundaryRequest) (*eventstore.GetBoundaryResponse, error) {
	validator := NewRequestValidator()
	if err := validator.ValidateGetBoundaryRequest(request); err != nil {
		return nil, err
	}

	response, err := c.adminClient.GetBoundary(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "getBoundary")
	}
	return response, nil
}

// CreateUser creates a new user
func (c *OrisunClient) CreateUser(ctx context.Context, request *eventstore.CreateUserRequest) (*eventstore.CreateUserResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateCreateUserRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Creating user with username: '{}'", request.Username)

	// Make the gRPC call
	response, err := c.adminClient.CreateUser(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "createUser")
	}

	c.logger.Info("Successfully created user with username: '{}'", request.Username)
	return response, nil
}

// DeleteUser deletes a user
func (c *OrisunClient) DeleteUser(ctx context.Context, request *eventstore.DeleteUserRequest) (*eventstore.DeleteUserResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateDeleteUserRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Deleting user with ID: '{}'", request.UserId)

	// Make the gRPC call
	response, err := c.adminClient.DeleteUser(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "deleteUser")
	}

	c.logger.Info("Successfully deleted user with ID: '{}'", request.UserId)
	return response, nil
}

// ChangePassword changes a user's password
func (c *OrisunClient) ChangePassword(ctx context.Context, request *eventstore.ChangePasswordRequest) (*eventstore.ChangePasswordResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateChangePasswordRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Changing password for user ID: '{}'", request.UserId)

	// Make the gRPC call
	response, err := c.adminClient.ChangePassword(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "changePassword")
	}

	c.logger.Info("Successfully changed password for user ID: '{}'", request.UserId)
	return response, nil
}

// SetUserBoundaryPermissions replaces one user's permissions for a boundary.
func (c *OrisunClient) SetUserBoundaryPermissions(
	ctx context.Context,
	request *eventstore.SetUserBoundaryPermissionsRequest,
) (*eventstore.SetUserBoundaryPermissionsResponse, error) {
	validator := NewRequestValidator()
	if err := validator.ValidateSetUserBoundaryPermissionsRequest(request); err != nil {
		return nil, err
	}

	response, err := c.adminClient.SetUserBoundaryPermissions(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "setUserBoundaryPermissions")
	}
	return response, nil
}

// ListUsers lists all users
func (c *OrisunClient) ListUsers(ctx context.Context, request *eventstore.ListUsersRequest) (*eventstore.ListUsersResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateListUsersRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Listing users")

	// Make the gRPC call
	response, err := c.adminClient.ListUsers(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "listUsers")
	}

	c.logger.Debug("Successfully retrieved {} users", len(response.Users))
	return response, nil
}

// ValidateCredentials validates user credentials
func (c *OrisunClient) ValidateCredentials(ctx context.Context, request *eventstore.ValidateCredentialsRequest) (*eventstore.ValidateCredentialsResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateValidateCredentialsRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Validating credentials for username: '{}'", request.Username)

	// Make the gRPC call
	response, err := c.adminClient.ValidateCredentials(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "validateCredentials")
	}

	c.logger.Debug("Successfully validated credentials for username: '{}'", request.Username)
	return response, nil
}

// GetUserCount gets the total number of users
func (c *OrisunClient) GetUserCount(ctx context.Context, request *eventstore.GetUserCountRequest) (*eventstore.GetUserCountResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateGetUserCountRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Getting user count")

	// Make the gRPC call
	response, err := c.adminClient.GetUserCount(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "getUserCount")
	}

	c.logger.Debug("Successfully retrieved user count: {}", response.Count)
	return response, nil
}

// GetEventCount gets the total number of events
func (c *OrisunClient) GetEventCount(ctx context.Context, request *eventstore.GetEventCountRequest) (*eventstore.GetEventCountResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateGetEventCountRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Getting event count")

	// Make the gRPC call
	response, err := c.adminClient.GetEventCount(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "getEventCount")
	}

	c.logger.Debug("Successfully retrieved event count: {}", response.Count)
	return response, nil
}

// CreateIndex creates an index on a boundary
func (c *OrisunClient) CreateIndex(ctx context.Context, request *eventstore.CreateIndexRequest) (*eventstore.CreateIndexResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateCreateIndexRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Creating index '{}' on boundary '{}'", request.Name, request.Boundary)

	// Make the gRPC call
	response, err := c.client.CreateIndex(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "createIndex")
	}

	c.logger.Info("Successfully created index '{}' on boundary '{}'", request.Name, request.Boundary)
	return response, nil
}

// DropIndex drops an index from a boundary
func (c *OrisunClient) DropIndex(ctx context.Context, request *eventstore.DropIndexRequest) (*eventstore.DropIndexResponse, error) {
	// Validate request
	validator := NewRequestValidator()
	if err := validator.ValidateDropIndexRequest(request); err != nil {
		return nil, err
	}

	c.logger.Debug("Dropping index '{}' from boundary '{}'", request.Name, request.Boundary)

	// Make the gRPC call
	response, err := c.client.DropIndex(ctx, request)
	if err != nil {
		return nil, c.handleAdminException(err, "dropIndex")
	}

	c.logger.Info("Successfully dropped index '{}' from boundary '{}'", request.Name, request.Boundary)
	return response, nil
}

// ListIndexes lists Orisun-managed indexes for a boundary.
func (c *OrisunClient) ListIndexes(ctx context.Context, boundary string) (*eventstore.ListIndexesResponse, error) {
	if strings.TrimSpace(boundary) == "" {
		return nil, NewOrisunException("Boundary is required").AddContext("operation", "listIndexes")
	}
	response, err := c.client.ListIndexes(ctx, &eventstore.ListIndexesRequest{Boundary: boundary})
	if err != nil {
		return nil, c.handleAdminException(err, "listIndexes")
	}
	return response, nil
}

// GetIndex gets one Orisun-managed index definition.
func (c *OrisunClient) GetIndex(ctx context.Context, boundary, name string) (*eventstore.GetIndexResponse, error) {
	if strings.TrimSpace(boundary) == "" {
		return nil, NewOrisunException("Boundary is required").AddContext("operation", "getIndex")
	}
	if strings.TrimSpace(name) == "" {
		return nil, NewOrisunException("Index name is required").
			AddContext("operation", "getIndex").
			AddContext("boundary", boundary)
	}
	response, err := c.client.GetIndex(ctx, &eventstore.GetIndexRequest{Boundary: boundary, Name: name})
	if err != nil {
		return nil, c.handleAdminException(err, "getIndex")
	}
	return response, nil
}

// handleAdminException handles exceptions from admin operations
func (c *OrisunClient) handleAdminException(err error, operation string) error {
	st, ok := status.FromError(err)
	if !ok {
		return NewOrisunExceptionWithCause(fmt.Sprintf("Failed to %s", operation), err).
			AddContext("operation", operation)
	}

	return NewOrisunExceptionWithCause(fmt.Sprintf("Failed to %s", operation), err).
		AddContext("operation", operation).
		AddContext("statusCode", st.Code().String()).
		AddContext("statusDescription", st.Message())
}
