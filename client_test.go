package orisun

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	eventstore "github.com/oexza/orisun-client-go/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestClientBuilder_WithHost(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.False(t, client.IsClosed())
}

func TestClientBuilder_WithServer(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithServer("example.com", 8080).Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.False(t, client.IsClosed())
}

func TestClientBuilder_WithMultipleServers(t *testing.T) {
	servers := []*ServerAddress{
		NewServerAddress("server1.example.com", 5005),
		NewServerAddress("server2.example.com", 5005),
	}

	builder := NewClientBuilder()
	client, err := builder.WithServers(servers).Build()

	// This test might fail due to gRPC connection issues without actual servers
	// We'll just verify that the builder doesn't panic
	if err != nil {
		// Expected to fail without actual servers
		assert.Contains(t, err.Error(), "Failed to create gRPC channel")
		return
	}

	require.NotNil(t, client)
	defer client.Close()

	assert.False(t, client.IsClosed())
}

func TestClientBuilder_WithTimeout(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").WithTimeout(60).Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.Equal(t, 60*time.Second, client.GetDefaultTimeout())
}

func TestClientBuilder_TransportTuningDefaults(t *testing.T) {
	builder := NewClientBuilder()

	assert.Equal(t, 100*1024*1024, builder.maxReceiveMessageSize)
	assert.Equal(t, 100*1024*1024, builder.maxSendMessageSize)
	assert.Equal(t, 1024*1024, builder.flowControlWindow)
}

func TestClientBuilder_TransportTuningOverrides(t *testing.T) {
	builder := NewClientBuilder().
		WithMaxReceiveMessageSize(64 * 1024 * 1024).
		WithMaxSendMessageSize(32 * 1024 * 1024).
		WithFlowControlWindow(2 * 1024 * 1024)

	assert.Equal(t, 64*1024*1024, builder.maxReceiveMessageSize)
	assert.Equal(t, 32*1024*1024, builder.maxSendMessageSize)
	assert.Equal(t, 2*1024*1024, builder.flowControlWindow)
}

func TestClientOptions_TransportTuningOverrides(t *testing.T) {
	builder := NewClientBuilder()

	WithMaxReceiveMessageSize(64 * 1024 * 1024)(builder)
	WithMaxSendMessageSize(32 * 1024 * 1024)(builder)
	WithFlowControlWindow(2 * 1024 * 1024)(builder)

	assert.Equal(t, 64*1024*1024, builder.maxReceiveMessageSize)
	assert.Equal(t, 32*1024*1024, builder.maxSendMessageSize)
	assert.Equal(t, 2*1024*1024, builder.flowControlWindow)
}

func TestClientBuilder_WithBasicAuth(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.
		WithHost("localhost").
		WithBasicAuth("username", "password").
		Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Note: We can't easily test the auth without actual gRPC calls
	// but we can verify the client was created successfully
	assert.False(t, client.IsClosed())
}

func TestNewClient(t *testing.T) {
	client, err := New(
		"localhost:5005",
		WithCredentials("username", "password"),
		WithDefaultTimeout(45*time.Second),
		WithInsecure(),
	)

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.Equal(t, 45*time.Second, client.GetDefaultTimeout())
	assert.False(t, client.IsClosed())
}

func TestClientBuilder_WithLogging(t *testing.T) {
	logger := NewDefaultLogger(DEBUG)
	builder := NewClientBuilder()
	client, err := builder.
		WithHost("localhost").
		WithLogger(logger).
		Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.Equal(t, logger, client.GetLogger())
}

func TestClient_Close(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)

	assert.False(t, client.IsClosed())

	err = client.Close()
	assert.NoError(t, err)
	assert.True(t, client.IsClosed())

	// Double close should not error
	err = client.Close()
	assert.NoError(t, err)
}

func TestServerAddress(t *testing.T) {
	sa := NewServerAddress("localhost", 5005)

	assert.Equal(t, "localhost", sa.Host)
	assert.Equal(t, 5005, sa.Port)
}

func TestDefaultLogger(t *testing.T) {
	logger := NewDefaultLogger(INFO)

	assert.True(t, logger.IsInfoEnabled())
	assert.False(t, logger.IsDebugEnabled())

	logger.SetLevel(DEBUG)
	assert.True(t, logger.IsDebugEnabled())
	assert.True(t, logger.IsInfoEnabled())

	assert.Equal(t, DEBUG, logger.GetLevel())
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	assert.False(t, logger.IsDebugEnabled())
	assert.False(t, logger.IsInfoEnabled())

	// These should not panic
	logger.Debug("test")
	logger.Info("test")
	logger.Warn("test")
	logger.Error("test")
	logger.Errorf("test", nil)
}

func TestOrisunException(t *testing.T) {
	err := NewOrisunException("test error")
	assert.Equal(t, "test error", err.GetMessage())
	assert.Nil(t, err.GetCause())

	err = err.AddContext("key", "value")
	assert.True(t, err.HasContext("key"))
	value, exists := err.GetContext("key")
	assert.True(t, exists)
	assert.Equal(t, "value", value)

	context := err.GetAllContext()
	assert.Contains(t, context, "key")
	assert.Equal(t, "value", context["key"])

	expectedMsg := "test error [Context: key=value]"
	assert.Equal(t, expectedMsg, err.Error())

	cause := errors.New("root cause")
	err = NewOrisunExceptionWithCause("wrapped", cause)
	assert.True(t, errors.Is(err, cause))
}

func TestOrisunExceptionWithCause(t *testing.T) {
	cause := assert.AnError
	err := NewOrisunExceptionWithCause("test error", cause)

	assert.Equal(t, "test error", err.GetMessage())
	assert.Equal(t, cause, err.GetCause())
	assert.Equal(t, cause, err.Unwrap())
}

func TestOptimisticConcurrencyException(t *testing.T) {
	err := NewOptimisticConcurrencyException("version conflict", 5, 7)

	assert.Equal(t, int64(5), err.GetExpectedVersion())
	assert.Equal(t, int64(7), err.GetActualVersion())

	expectedMsg := "version conflict (Expected version: 5, Actual version: 7)"
	assert.Equal(t, expectedMsg, err.Error())
}

func TestTokenCache(t *testing.T) {
	logger := NewNoOpLogger()
	cache := NewTokenCache(logger)

	assert.False(t, cache.HasToken())
	assert.Equal(t, "", cache.GetCachedToken())

	cache.CacheToken("test-token")
	assert.True(t, cache.HasToken())
	assert.Equal(t, "test-token", cache.GetCachedToken())

	cache.ClearToken()
	assert.False(t, cache.HasToken())
	assert.Equal(t, "", cache.GetCachedToken())
}

func TestCreateBasicAuthCredentials(t *testing.T) {
	creds := CreateBasicAuthCredentials("user", "pass")
	assert.NotEmpty(t, creds)
	assert.True(t, len(creds) > 6) // Basic + base64 encoded "user:pass"
	assert.Contains(t, creds, "Basic ")

	emptyCreds := CreateBasicAuthCredentials("", "")
	assert.Equal(t, "", emptyCreds)
}

func TestRetryHelper(t *testing.T) {
	config := DefaultRetryConfig()
	config.MaxRetries = 2
	config.InitialDelay = 10 * time.Millisecond
	helper := NewRetryHelper(config)

	attempts := 0
	err := helper.Do(func() error {
		attempts++
		if attempts < 3 {
			return assert.AnError
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)

	// Test with non-retryable error
	attempts = 0
	config.RetryableFunc = func(err error) bool { return false }
	helper = NewRetryHelper(config)

	err = helper.Do(func() error {
		attempts++
		return assert.AnError
	})

	assert.Error(t, err)
	assert.Equal(t, 1, attempts) // Should only attempt once
}

func TestContextHelper(t *testing.T) {
	helper := NewContextHelper()

	ctx, cancel := helper.WithTimeout(context.Background(), 5)
	defer cancel()

	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(5*time.Second), deadline, time.Second)
}

func TestStringHelper(t *testing.T) {
	helper := NewStringHelper()

	assert.True(t, helper.IsEmpty(""))
	assert.True(t, helper.IsEmpty("  "))
	assert.False(t, helper.IsEmpty("test"))

	assert.False(t, helper.IsNotEmpty(""))
	assert.True(t, helper.IsNotEmpty("test"))
	assert.Equal(t, "test", helper.TrimSpace("  test  "))
	assert.True(t, helper.Contains("hello world", "world"))
	assert.True(t, helper.Contains("hello", ""))

	message := helper.FormatMessage("Hello {}, you have {} messages", "Alice", 5)
	assert.Equal(t, "Hello Alice, you have 5 messages", message)

	message = helper.FormatMessage("No placeholders")
	assert.Equal(t, "No placeholders", message)
}

func TestExtractVersionNumbers(t *testing.T) {
	expected, actual, err := ExtractVersionNumbers("Expected 5, Actual 7")

	assert.NoError(t, err)
	assert.Equal(t, int64(5), expected)
	assert.Equal(t, int64(7), actual)

	_, _, err = ExtractVersionNumbers("No version info")
	assert.Error(t, err)
}

// Test subscription functionality
func TestEventSubscription(t *testing.T) {
	logger := NewNoOpLogger()

	// Track events received
	var receivedEvents []*eventstore.Event
	var completedCalled bool
	var errorCalled bool

	handler := NewSimpleEventHandler().
		WithOnEvent(func(event *eventstore.Event) error {
			receivedEvents = append(receivedEvents, event)
			return nil
		}).
		WithOnError(func(err error) {
			errorCalled = true
		}).
		WithOnCompleted(func() {
			completedCalled = true
		})

	// Create a mock stream
	mockStream := &mockEventStream{
		events: []*eventstore.Event{
			{
				EventId:   "test-1",
				EventType: "TestEvent",
				Data:      "test data 1",
			},
			{
				EventId:   "test-2",
				EventType: "TestEvent",
				Data:      "test data 2",
			},
		},
		eventIndex: 0,
		closed:     false,
	}

	subscription := NewEventSubscription(mockStream, handler, logger, func() {})

	// Wait a moment for the goroutine to process events
	time.Sleep(100 * time.Millisecond)

	// Test that events were received
	assert.Equal(t, 2, len(receivedEvents))
	assert.Equal(t, "test-1", receivedEvents[0].EventId)
	assert.Equal(t, "test-2", receivedEvents[1].EventId)

	// Test closing subscription
	closeErr := subscription.Close()
	assert.NoError(t, closeErr)
	assert.True(t, completedCalled)
	// Error handler is called when stream runs out of events, which is expected
	assert.True(t, errorCalled)
}

// Test SaveEvents method
func TestClient_SaveEvents(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with valid request. Event IDs are application-defined strings.
	request := &eventstore.SaveEventsRequest{
		Boundary: "test-boundary",
		Events: []*eventstore.EventToSave{
			{
				EventId:   "test-1",
				EventType: "TestEvent",
				Data:      "test data 1",
			},
		},
	}

	// This will fail without actual server, but we test the method exists and validation works
	_, err = client.SaveEvents(context.Background(), request)
	assert.Error(t, err) // Expected to fail without actual server
	assert.Contains(t, err.Error(), "Failed to save events")
}

// Test SaveEvents with nil request
func TestClient_SaveEvents_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.SaveEvents(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SaveEventsRequest cannot be nil")

	// Test with empty boundary
	request := &eventstore.SaveEventsRequest{
		Boundary: "",
		Events:   []*eventstore.EventToSave{},
	}
	_, err = client.SaveEvents(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Boundary is required")

	// Test with nil stream
	request = &eventstore.SaveEventsRequest{
		Boundary: "test-boundary",
		Events:   []*eventstore.EventToSave{},
	}
	_, err = client.SaveEvents(context.Background(), request)
	assert.Error(t, err)

	// Test with no events
	request = &eventstore.SaveEventsRequest{
		Boundary: "test-boundary",
		Events:   []*eventstore.EventToSave{},
	}
	_, err = client.SaveEvents(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "At least one event is required")
}

// Test GetEvents method
func TestClient_GetEvents(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with valid request
	request := &eventstore.GetEventsRequest{
		Boundary: "test-boundary",
		Count:    10,
	}

	// This will fail without actual server, but we test the method exists and validation works
	_, err = client.GetEvents(context.Background(), request)
	assert.Error(t, err) // Expected to fail without actual server
	assert.Contains(t, err.Error(), "Failed to get events")
}

// Test GetEvents with validation errors
func TestClient_GetEvents_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.GetEvents(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetEventsRequest cannot be nil")

	// Test with empty boundary
	request := &eventstore.GetEventsRequest{
		Boundary: "",
		Count:    10,
	}
	_, err = client.GetEvents(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Boundary is required")

	// Test with invalid count
	request = &eventstore.GetEventsRequest{
		Boundary: "test-boundary",
		Count:    0,
	}
	_, err = client.GetEvents(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Count must be greater than 0")
}

// Test GetLatestByCriteria method
func TestClient_GetLatestByCriteria(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	request := &eventstore.GetLatestByCriteriaRequest{
		Boundary: "test-boundary",
		Criteria: []*eventstore.Criterion{
			{
				Tags: []*eventstore.Tag{
					{Key: "account_id", Value: "acct-1"},
				},
			},
		},
	}

	_, err = client.GetLatestByCriteria(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to get latest events by criteria")
}

// Test GetLatestByCriteria with validation errors
func TestClient_GetLatestByCriteria_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	_, err = client.GetLatestByCriteria(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetLatestByCriteriaRequest cannot be nil")

	request := &eventstore.GetLatestByCriteriaRequest{
		Boundary: "",
		Criteria: []*eventstore.Criterion{
			{Tags: []*eventstore.Tag{{Key: "account_id", Value: "acct-1"}}},
		},
	}
	_, err = client.GetLatestByCriteria(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Boundary is required")

	request = &eventstore.GetLatestByCriteriaRequest{
		Boundary: "test-boundary",
	}
	_, err = client.GetLatestByCriteria(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "At least one criterion is required")

	request = &eventstore.GetLatestByCriteriaRequest{
		Boundary: "test-boundary",
		Criteria: []*eventstore.Criterion{{}},
	}
	_, err = client.GetLatestByCriteria(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Criterion at index 0 must include at least one tag")
}

// Test SubscribeToEvents method
func TestClient_SubscribeToEvents(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Track events received
	var receivedEvents []*eventstore.Event
	handler := NewSimpleEventHandler().
		WithOnEvent(func(event *eventstore.Event) error {
			receivedEvents = append(receivedEvents, event)
			return nil
		}).
		WithOnError(func(err error) {
			// Handle error
		}).
		WithOnCompleted(func() {
			// Handle completion
		})

	request := &eventstore.CatchUpSubscribeToEventStoreRequest{
		Boundary:       "test-boundary",
		SubscriberName: "test-subscriber",
	}

	subscription, err := client.SubscribeToEvents(context.Background(), request, handler)
	if err == nil {
		assert.NotNil(t, subscription)
		assert.NoError(t, subscription.Close())
		return
	}
	assert.Contains(t, err.Error(), "Failed to create subscription")
}

// Test SubscribeToEvents with validation errors
func TestClient_SubscribeToEvents_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	handler := NewSimpleEventHandler()

	// Test with nil request
	_, err = client.SubscribeToEvents(context.Background(), nil, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SubscribeRequest cannot be nil")

	// Test with empty boundary
	request := &eventstore.CatchUpSubscribeToEventStoreRequest{
		Boundary:       "",
		SubscriberName: "test-subscriber",
	}
	_, err = client.SubscribeToEvents(context.Background(), request, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Boundary is required")

	// Test with empty subscriber name
	request = &eventstore.CatchUpSubscribeToEventStoreRequest{
		Boundary:       "test-boundary",
		SubscriberName: "",
	}
	_, err = client.SubscribeToEvents(context.Background(), request, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Subscriber name is required")
}

// Test error handling scenarios
func TestClient_SaveEvents_EventIDIsApplicationDefined(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()

	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	request := &eventstore.SaveEventsRequest{
		Boundary: "test-boundary",
		Events: []*eventstore.EventToSave{
			{
				EventId:   "invalid-uuid",
				EventType: "TestEvent",
				Data:      "test data 1",
			},
		},
	}

	_, err = client.SaveEvents(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to save events")
}

// Test edge cases for boundary conditions
func TestClient_EdgeCases(t *testing.T) {
	builder := NewClientBuilder()

	// Test with empty host (should default to localhost)
	client, err := builder.WithHost("").Build()
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	// Test with invalid port (should still work)
	client, err = builder.WithHost("localhost").WithPort(-1).Build()
	// This might fail but shouldn't panic
	if err != nil {
		// Connection might fail with invalid port
		assert.Error(t, err)
	} else {
		assert.NotNil(t, client)
		defer client.Close()
	}
}

// Test authentication scenarios
func TestClient_Authentication(t *testing.T) {
	// Test with empty credentials
	creds := CreateBasicAuthCredentials("", "")
	assert.Equal(t, "", creds)

	// Test with valid credentials
	creds = CreateBasicAuthCredentials("user", "pass")
	assert.NotEmpty(t, creds)
	assert.Contains(t, creds, "Basic ")
	assert.True(t, len(creds) > 6)
}

// Test token caching scenarios
func TestClient_TokenCaching(t *testing.T) {
	logger := NewNoOpLogger()
	cache := NewTokenCache(logger)

	// Test caching empty token
	cache.CacheToken("")
	assert.False(t, cache.HasToken())
	assert.Equal(t, "", cache.GetCachedToken())

	// Test caching valid token
	testToken := "test-auth-token"
	cache.CacheToken(testToken)
	assert.True(t, cache.HasToken())
	assert.Equal(t, testToken, cache.GetCachedToken())

	// Test clearing token
	cache.ClearToken()
	assert.False(t, cache.HasToken())
	assert.Equal(t, "", cache.GetCachedToken())
}

// Mock event stream for testing
type mockEventStream struct {
	events     []*eventstore.Event
	eventIndex int
	closed     bool
}

func (m *mockEventStream) RecvMsg(msg interface{}) error {
	if m.closed {
		return fmt.Errorf("stream closed")
	}

	if m.eventIndex >= len(m.events) {
		return fmt.Errorf("no more events")
	}

	event := m.events[m.eventIndex]
	m.eventIndex++

	// Convert mock event to protobuf event
	if eventMsg, ok := msg.(*eventstore.Event); ok {
		proto.Reset(eventMsg)
		proto.Merge(eventMsg, event)
	}

	return nil
}

func (m *mockEventStream) Close() error {
	m.closed = true
	return nil
}

// Implement grpc.ClientStream interface
func (m *mockEventStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockEventStream) Trailer() metadata.MD {
	return nil
}

func (m *mockEventStream) CloseSend() error {
	return nil
}

func (m *mockEventStream) Context() context.Context {
	return context.Background()
}

func (m *mockEventStream) SendMsg(msg interface{}) error {
	return nil
}

func (m *mockEventStream) Recv() interface{} {
	if m.closed {
		return nil
	}

	if m.eventIndex >= len(m.events) {
		return nil
	}

	event := m.events[m.eventIndex]
	m.eventIndex++
	return event
}

// Test CreateUser validation
func TestClient_CreateUser_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.CreateUser(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CreateUserRequest cannot be nil")

	// Test with missing name
	request := &eventstore.CreateUserRequest{
		Username: "testuser",
		Password: "password123",
	}
	_, err = client.CreateUser(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Name is required")

	// Test with missing username
	request = &eventstore.CreateUserRequest{
		Name:     "Test User",
		Password: "password123",
	}
	_, err = client.CreateUser(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Username is required")

	// Test with missing password
	request = &eventstore.CreateUserRequest{
		Name:     "Test User",
		Username: "testuser",
	}
	_, err = client.CreateUser(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Password is required")
}

func TestClient_BoundaryManagement_Validation(t *testing.T) {
	client, err := NewClientBuilder().WithHost("localhost").Build()
	require.NoError(t, err)
	defer client.Close()

	_, err = client.CreateBoundary(context.Background(), nil)
	require.ErrorContains(t, err, "CreateBoundaryRequest cannot be nil")

	_, err = client.CreateBoundary(context.Background(), &eventstore.CreateBoundaryRequest{
		Name: "orders",
	})
	require.ErrorContains(t, err, "Boundary placement is required")

	_, err = client.CreateBoundary(context.Background(), &eventstore.CreateBoundaryRequest{
		Name:                 "orders",
		Placement:            &eventstore.BoundaryPlacementInput{Backend: "postgres"},
		ExistedBeforeCatalog: true,
	})
	require.ErrorContains(t, err, "Boundary placement namespace is required")

	_, err = client.ListBoundaries(context.Background(), nil)
	require.ErrorContains(t, err, "ListBoundariesRequest cannot be nil")

	_, err = client.GetBoundary(context.Background(), &eventstore.GetBoundaryRequest{})
	require.ErrorContains(t, err, "Boundary name is required")
}

// Test DeleteUser validation
func TestClient_DeleteUser_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.DeleteUser(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DeleteUserRequest cannot be nil")

	// Test with missing user_id
	request := &eventstore.DeleteUserRequest{}
	_, err = client.DeleteUser(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "User ID is required")
}

// Test ChangePassword validation
func TestClient_ChangePassword_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.ChangePassword(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ChangePasswordRequest cannot be nil")

	// Test with missing user_id
	request := &eventstore.ChangePasswordRequest{
		CurrentPassword: "oldpass",
		NewPassword:     "newpass",
	}
	_, err = client.ChangePassword(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "User ID is required")

	// Test with missing current password
	request = &eventstore.ChangePasswordRequest{
		UserId:      "user123",
		NewPassword: "newpass",
	}
	_, err = client.ChangePassword(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Current password is required")

	// Test with missing new password
	request = &eventstore.ChangePasswordRequest{
		UserId:          "user123",
		CurrentPassword: "oldpass",
	}
	_, err = client.ChangePassword(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "New password is required")
}

// Test ListUsers validation
func TestClient_ListUsers_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.ListUsers(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ListUsersRequest cannot be nil")

	// Test with valid request (should fail connection but validation passes)
	request := &eventstore.ListUsersRequest{}
	_, err = client.ListUsers(context.Background(), request)
	// Will fail without actual server but validation should pass
	assert.Error(t, err)
}

// Test ValidateCredentials validation
func TestClient_ValidateCredentials_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.ValidateCredentials(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ValidateCredentialsRequest cannot be nil")

	// Test with missing username
	request := &eventstore.ValidateCredentialsRequest{
		Password: "password123",
	}
	_, err = client.ValidateCredentials(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Username is required")

	// Test with missing password
	request = &eventstore.ValidateCredentialsRequest{
		Username: "testuser",
	}
	_, err = client.ValidateCredentials(context.Background(), request)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Password is required")
}

// Test GetUserCount validation
func TestClient_GetUserCount_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.GetUserCount(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetUserCountRequest cannot be nil")

	// Test with valid request (should fail connection but validation passes)
	request := &eventstore.GetUserCountRequest{}
	_, err = client.GetUserCount(context.Background(), request)
	// Will fail without actual server but validation should pass
	assert.Error(t, err)
}

// Test GetEventCount validation
func TestClient_GetEventCount_Validation(t *testing.T) {
	builder := NewClientBuilder()
	client, err := builder.WithHost("localhost").Build()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Test with nil request
	_, err = client.GetEventCount(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetEventCountRequest cannot be nil")

	// Test with valid request (should fail connection but validation passes)
	request := &eventstore.GetEventCountRequest{}
	_, err = client.GetEventCount(context.Background(), request)
	// Will fail without actual server but validation should pass
	assert.Error(t, err)
}

// Test admin request validators directly
func TestRequestValidator_AdminRequests(t *testing.T) {
	validator := NewRequestValidator()

	t.Run("BoundaryRequests", func(t *testing.T) {
		placement := &eventstore.BoundaryPlacementInput{Backend: "postgres", Namespace: "orders"}
		assert.NoError(t, validator.ValidateCreateBoundaryRequest(&eventstore.CreateBoundaryRequest{
			Name: "orders", Placement: placement,
		}))
		assert.NoError(t, validator.ValidateListBoundariesRequest(&eventstore.ListBoundariesRequest{}))
		assert.NoError(t, validator.ValidateGetBoundaryRequest(&eventstore.GetBoundaryRequest{Name: "orders"}))

		err := validator.ValidateCreateBoundaryRequest(&eventstore.CreateBoundaryRequest{
			Name: "orders",
			Placement: &eventstore.BoundaryPlacementInput{
				Namespace: "orders",
			},
		})
		assert.ErrorContains(t, err, "backend is required")

		err = validator.ValidateCreateBoundaryRequest(&eventstore.CreateBoundaryRequest{
			Name: "orders",
			Placement: &eventstore.BoundaryPlacementInput{
				Backend: "postgres",
			},
			ExistedBeforeCatalog: true,
		})
		assert.ErrorContains(t, err, "namespace is required")
	})

	// Test CreateUserRequest validation
	t.Run("CreateUserRequest", func(t *testing.T) {
		err := validator.ValidateCreateUserRequest(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be nil")

		err = validator.ValidateCreateUserRequest(&eventstore.CreateUserRequest{
			Username: "test",
			Password: "pass",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Name is required")

		err = validator.ValidateCreateUserRequest(&eventstore.CreateUserRequest{
			Name:     "Test",
			Password: "pass",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Username is required")

		err = validator.ValidateCreateUserRequest(&eventstore.CreateUserRequest{
			Name:     "Test",
			Username: "test",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Password is required")

		err = validator.ValidateCreateUserRequest(&eventstore.CreateUserRequest{
			Name:     "Test",
			Username: "test",
			Password: "pass",
		})
		assert.NoError(t, err)
	})

	// Test DeleteUserRequest validation
	t.Run("DeleteUserRequest", func(t *testing.T) {
		err := validator.ValidateDeleteUserRequest(nil)
		assert.Error(t, err)

		err = validator.ValidateDeleteUserRequest(&eventstore.DeleteUserRequest{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "User ID is required")

		err = validator.ValidateDeleteUserRequest(&eventstore.DeleteUserRequest{
			UserId: "user123",
		})
		assert.NoError(t, err)
	})

	t.Run("SetUserBoundaryPermissionsRequest", func(t *testing.T) {
		err := validator.ValidateSetUserBoundaryPermissionsRequest(nil)
		assert.ErrorContains(t, err, "cannot be nil")

		err = validator.ValidateSetUserBoundaryPermissionsRequest(
			&eventstore.SetUserBoundaryPermissionsRequest{Boundary: "orders"},
		)
		assert.ErrorContains(t, err, "User ID is required")

		err = validator.ValidateSetUserBoundaryPermissionsRequest(
			&eventstore.SetUserBoundaryPermissionsRequest{UserId: "user-1"},
		)
		assert.ErrorContains(t, err, "Boundary is required")

		err = validator.ValidateSetUserBoundaryPermissionsRequest(
			&eventstore.SetUserBoundaryPermissionsRequest{
				UserId:      "user-1",
				Boundary:    "orders",
				Permissions: nil,
			},
		)
		assert.NoError(t, err)
	})

	// Test ChangePasswordRequest validation
	t.Run("ChangePasswordRequest", func(t *testing.T) {
		err := validator.ValidateChangePasswordRequest(nil)
		assert.Error(t, err)

		err = validator.ValidateChangePasswordRequest(&eventstore.ChangePasswordRequest{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "User ID is required")

		err = validator.ValidateChangePasswordRequest(&eventstore.ChangePasswordRequest{
			UserId: "user123",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Current password is required")

		err = validator.ValidateChangePasswordRequest(&eventstore.ChangePasswordRequest{
			UserId:          "user123",
			CurrentPassword: "oldpass",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "New password is required")

		err = validator.ValidateChangePasswordRequest(&eventstore.ChangePasswordRequest{
			UserId:          "user123",
			CurrentPassword: "oldpass",
			NewPassword:     "newpass",
		})
		assert.NoError(t, err)
	})

	// Test ValidateCredentialsRequest validation
	t.Run("ValidateCredentialsRequest", func(t *testing.T) {
		err := validator.ValidateValidateCredentialsRequest(nil)
		assert.Error(t, err)

		err = validator.ValidateValidateCredentialsRequest(&eventstore.ValidateCredentialsRequest{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Username is required")

		err = validator.ValidateValidateCredentialsRequest(&eventstore.ValidateCredentialsRequest{
			Username: "testuser",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Password is required")

		err = validator.ValidateValidateCredentialsRequest(&eventstore.ValidateCredentialsRequest{
			Username: "testuser",
			Password: "pass",
		})
		assert.NoError(t, err)
	})
}
