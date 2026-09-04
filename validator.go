package orisun

import (
	"fmt"
	"regexp"
	"strings"

	eventstore "github.com/oexza/orisun-client-go/eventstore"
)

// RequestValidator provides validation methods for various request types
type RequestValidator struct{}

// NewRequestValidator creates a new RequestValidator
func NewRequestValidator() *RequestValidator {
	return &RequestValidator{}
}

// ValidateSaveEventsRequest validates a SaveEventsRequest
func (v *RequestValidator) ValidateSaveEventsRequest(request *eventstore.SaveEventsRequest) error {
	if request == nil {
		return NewOrisunException("SaveEventsRequest cannot be nil").
			AddContext("operation", "saveEvents")
	}

	return v.validateSaveRequest(request.Boundary, request.Events, "saveEvents", "SaveEventsRequest")
}

// ValidateSaveEventsV2Request validates a SaveEventsV2Request.
func (v *RequestValidator) ValidateSaveEventsV2Request(request *eventstore.SaveEventsV2Request) error {
	if request == nil {
		return NewOrisunException("SaveEventsV2Request cannot be nil").
			AddContext("operation", "saveEventsV2")
	}

	if err := v.validateSaveRequest(request.Boundary, request.Events, "saveEventsV2", "SaveEventsV2Request"); err != nil {
		return err
	}

	for observationIndex, observation := range request.Consistency {
		if observation == nil || observation.Query == nil || observation.Position == nil {
			return NewOrisunException(fmt.Sprintf("Consistency observation at index %d must include query and position", observationIndex)).
				AddContext("operation", "saveEventsV2").
				AddContext("observationIndex", observationIndex).
				AddContext("boundary", request.Boundary)
		}
		if len(observation.Query.Criteria) == 0 {
			return NewOrisunException(fmt.Sprintf("Consistency observation at index %d must include at least one criterion", observationIndex)).
				AddContext("operation", "saveEventsV2").
				AddContext("observationIndex", observationIndex).
				AddContext("boundary", request.Boundary)
		}
		for criterionIndex, criterion := range observation.Query.Criteria {
			if criterion == nil || len(criterion.Tags) == 0 {
				return NewOrisunException(fmt.Sprintf("Criterion at index %d in consistency observation %d must include at least one tag", criterionIndex, observationIndex)).
					AddContext("operation", "saveEventsV2").
					AddContext("observationIndex", observationIndex).
					AddContext("criterionIndex", criterionIndex).
					AddContext("boundary", request.Boundary)
			}
		}
	}

	return nil
}

func (v *RequestValidator) validateSaveRequest(
	boundary string,
	events []*eventstore.EventToSave,
	operation string,
	requestName string,
) error {
	if strings.TrimSpace(boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", operation).
			AddContext("request", requestName)
	}
	if len(events) == 0 {
		return NewOrisunException("At least one event is required").
			AddContext("operation", operation).
			AddContext("boundary", boundary)
	}
	for i, event := range events {
		if err := v.validateEventToSave(event, i, boundary, operation); err != nil {
			return err
		}
	}
	return nil
}

// validateEventToSave validates an EventToSave
func (v *RequestValidator) validateEventToSave(event *eventstore.EventToSave, index int, boundary, operation string) error {
	if event == nil {
		return NewOrisunException(fmt.Sprintf("Event at index %d is nil", index)).
			AddContext("operation", operation).
			AddContext("eventIndex", index).
			AddContext("boundary", boundary)
	}

	// Validate eventId
	if strings.TrimSpace(event.EventId) == "" {
		return NewOrisunException(fmt.Sprintf("Event at index %d is missing eventId", index)).
			AddContext("operation", operation).
			AddContext("eventIndex", index).
			AddContext("boundary", boundary)
	}

	// Validate eventType
	if strings.TrimSpace(event.EventType) == "" {
		return NewOrisunException(fmt.Sprintf("Event at index %d is missing eventType", index)).
			AddContext("operation", operation).
			AddContext("eventIndex", index).
			AddContext("boundary", boundary)
	}

	// Validate data
	if strings.TrimSpace(event.Data) == "" {
		return NewOrisunException(fmt.Sprintf("Event at index %d is missing data", index)).
			AddContext("operation", operation).
			AddContext("eventIndex", index).
			AddContext("boundary", boundary)
	}

	return nil
}

// ValidateGetEventsRequest validates a GetEventsRequest
func (v *RequestValidator) ValidateGetEventsRequest(request *eventstore.GetEventsRequest) error {
	if request == nil {
		return NewOrisunException("GetEventsRequest cannot be nil").
			AddContext("operation", "getEvents")
	}

	// Validate boundary
	if strings.TrimSpace(request.Boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", "getEvents").
			AddContext("request", "GetEventsRequest")
	}

	// Validate count if provided
	if request.Count <= 0 {
		return NewOrisunException("Count must be greater than 0").
			AddContext("operation", "getEvents").
			AddContext("count", request.Count).
			AddContext("boundary", request.Boundary)
	}

	return nil
}

// ValidateGetLatestByCriteriaRequest validates a GetLatestByCriteriaRequest
func (v *RequestValidator) ValidateGetLatestByCriteriaRequest(request *eventstore.GetLatestByCriteriaRequest) error {
	if request == nil {
		return NewOrisunException("GetLatestByCriteriaRequest cannot be nil").
			AddContext("operation", "getLatestByCriteria")
	}

	if strings.TrimSpace(request.Boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", "getLatestByCriteria").
			AddContext("request", "GetLatestByCriteriaRequest")
	}

	if len(request.Criteria) == 0 {
		return NewOrisunException("At least one criterion is required").
			AddContext("operation", "getLatestByCriteria").
			AddContext("boundary", request.Boundary)
	}

	for i, criterion := range request.Criteria {
		if criterion == nil || len(criterion.Tags) == 0 {
			return NewOrisunException(fmt.Sprintf("Criterion at index %d must include at least one tag", i)).
				AddContext("operation", "getLatestByCriteria").
				AddContext("criterionIndex", i).
				AddContext("boundary", request.Boundary)
		}
	}

	return nil
}

// ValidateSubscribeRequest validates a CatchUpSubscribeToEventStoreRequest
func (v *RequestValidator) ValidateSubscribeRequest(request *eventstore.CatchUpSubscribeToEventStoreRequest) error {
	if request == nil {
		return NewOrisunException("SubscribeRequest cannot be nil").
			AddContext("operation", "subscribeToEvents")
	}

	// Validate boundary
	if strings.TrimSpace(request.Boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", "subscribeToEvents").
			AddContext("request", "CatchUpSubscribeToEventStoreRequest")
	}

	// Validate subscriber name
	if strings.TrimSpace(request.SubscriberName) == "" {
		return NewOrisunException("Subscriber name is required").
			AddContext("operation", "subscribeToEvents").
			AddContext("boundary", request.Boundary)
	}

	return nil
}

// ValidateCreateBoundaryRequest validates a CreateBoundaryRequest.
func (v *RequestValidator) ValidateCreateBoundaryRequest(request *eventstore.CreateBoundaryRequest) error {
	if request == nil {
		return NewOrisunException("CreateBoundaryRequest cannot be nil").
			AddContext("operation", "createBoundary")
	}
	return v.validateBoundaryDefinition(
		request.Name,
		request.Placement,
		"createBoundary",
	)
}

func (v *RequestValidator) validateBoundaryDefinition(
	name string,
	placement *eventstore.BoundaryPlacementInput,
	operation string,
) error {
	if strings.TrimSpace(name) == "" {
		return NewOrisunException("Boundary name is required").
			AddContext("operation", operation)
	}
	if placement == nil {
		return NewOrisunException("Boundary placement is required").
			AddContext("operation", operation).
			AddContext("boundary", name)
	}
	if strings.TrimSpace(placement.Backend) == "" {
		return NewOrisunException("Boundary placement backend is required").
			AddContext("operation", operation).
			AddContext("boundary", name)
	}
	if strings.TrimSpace(placement.Namespace) == "" {
		return NewOrisunException("Boundary placement namespace is required").
			AddContext("operation", operation).
			AddContext("boundary", name)
	}
	return nil
}

// ValidateListBoundariesRequest validates a ListBoundariesRequest.
func (v *RequestValidator) ValidateListBoundariesRequest(request *eventstore.ListBoundariesRequest) error {
	if request == nil {
		return NewOrisunException("ListBoundariesRequest cannot be nil").
			AddContext("operation", "listBoundaries")
	}
	return nil
}

// ValidateGetBoundaryRequest validates a GetBoundaryRequest.
func (v *RequestValidator) ValidateGetBoundaryRequest(request *eventstore.GetBoundaryRequest) error {
	if request == nil {
		return NewOrisunException("GetBoundaryRequest cannot be nil").
			AddContext("operation", "getBoundary")
	}
	if strings.TrimSpace(request.Name) == "" {
		return NewOrisunException("Boundary name is required").
			AddContext("operation", "getBoundary")
	}
	return nil
}

// ValidateCreateUserRequest validates a CreateUserRequest
func (v *RequestValidator) ValidateCreateUserRequest(request *eventstore.CreateUserRequest) error {
	if request == nil {
		return NewOrisunException("CreateUserRequest cannot be nil").
			AddContext("operation", "createUser")
	}

	// Validate name
	if strings.TrimSpace(request.Name) == "" {
		return NewOrisunException("Name is required").
			AddContext("operation", "createUser")
	}

	// Validate username
	if strings.TrimSpace(request.Username) == "" {
		return NewOrisunException("Username is required").
			AddContext("operation", "createUser")
	}

	// Validate password
	if strings.TrimSpace(request.Password) == "" {
		return NewOrisunException("Password is required").
			AddContext("operation", "createUser")
	}

	return nil
}

// ValidateDeleteUserRequest validates a DeleteUserRequest
func (v *RequestValidator) ValidateDeleteUserRequest(request *eventstore.DeleteUserRequest) error {
	if request == nil {
		return NewOrisunException("DeleteUserRequest cannot be nil").
			AddContext("operation", "deleteUser")
	}

	// Validate user_id
	if strings.TrimSpace(request.UserId) == "" {
		return NewOrisunException("User ID is required").
			AddContext("operation", "deleteUser")
	}

	return nil
}

// ValidateSetUserBoundaryPermissionsRequest validates a boundary grant replacement.
func (v *RequestValidator) ValidateSetUserBoundaryPermissionsRequest(
	request *eventstore.SetUserBoundaryPermissionsRequest,
) error {
	if request == nil {
		return NewOrisunException("SetUserBoundaryPermissionsRequest cannot be nil").
			AddContext("operation", "setUserBoundaryPermissions")
	}
	if strings.TrimSpace(request.UserId) == "" {
		return NewOrisunException("User ID is required").
			AddContext("operation", "setUserBoundaryPermissions")
	}
	if strings.TrimSpace(request.Boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", "setUserBoundaryPermissions")
	}
	return nil
}

// ValidateChangePasswordRequest validates a ChangePasswordRequest
func (v *RequestValidator) ValidateChangePasswordRequest(request *eventstore.ChangePasswordRequest) error {
	if request == nil {
		return NewOrisunException("ChangePasswordRequest cannot be nil").
			AddContext("operation", "changePassword")
	}

	// Validate user_id
	if strings.TrimSpace(request.UserId) == "" {
		return NewOrisunException("User ID is required").
			AddContext("operation", "changePassword")
	}

	// Validate current_password
	if strings.TrimSpace(request.CurrentPassword) == "" {
		return NewOrisunException("Current password is required").
			AddContext("operation", "changePassword")
	}

	// Validate new_password
	if strings.TrimSpace(request.NewPassword) == "" {
		return NewOrisunException("New password is required").
			AddContext("operation", "changePassword")
	}

	return nil
}

// ValidateListUsersRequest validates a ListUsersRequest
func (v *RequestValidator) ValidateListUsersRequest(request *eventstore.ListUsersRequest) error {
	if request == nil {
		return NewOrisunException("ListUsersRequest cannot be nil").
			AddContext("operation", "listUsers")
	}

	// ListUsersRequest has no required fields
	return nil
}

// ValidateValidateCredentialsRequest validates a ValidateCredentialsRequest
func (v *RequestValidator) ValidateValidateCredentialsRequest(request *eventstore.ValidateCredentialsRequest) error {
	if request == nil {
		return NewOrisunException("ValidateCredentialsRequest cannot be nil").
			AddContext("operation", "validateCredentials")
	}

	// Validate username
	if strings.TrimSpace(request.Username) == "" {
		return NewOrisunException("Username is required").
			AddContext("operation", "validateCredentials")
	}

	// Validate password
	if strings.TrimSpace(request.Password) == "" {
		return NewOrisunException("Password is required").
			AddContext("operation", "validateCredentials")
	}

	return nil
}

// ValidateGetUserCountRequest validates a GetUserCountRequest
func (v *RequestValidator) ValidateGetUserCountRequest(request *eventstore.GetUserCountRequest) error {
	if request == nil {
		return NewOrisunException("GetUserCountRequest cannot be nil").
			AddContext("operation", "getUserCount")
	}

	// GetUserCountRequest has no required fields
	return nil
}

// ValidateGetEventCountRequest validates a GetEventCountRequest
func (v *RequestValidator) ValidateGetEventCountRequest(request *eventstore.GetEventCountRequest) error {
	if request == nil {
		return NewOrisunException("GetEventCountRequest cannot be nil").
			AddContext("operation", "getEventCount")
	}

	// GetEventCountRequest has no required fields
	return nil
}

// ValidateCreateIndexRequest validates a CreateIndexRequest
func (v *RequestValidator) ValidateCreateIndexRequest(request *eventstore.CreateIndexRequest) error {
	if request == nil {
		return NewOrisunException("CreateIndexRequest cannot be nil").
			AddContext("operation", "createIndex")
	}

	// Validate boundary
	if strings.TrimSpace(request.Boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", "createIndex")
	}

	// Validate name
	if strings.TrimSpace(request.Name) == "" {
		return NewOrisunException("Index name is required").
			AddContext("operation", "createIndex")
	}

	// Validate fields
	if len(request.Fields) == 0 {
		return NewOrisunException("At least one field is required").
			AddContext("operation", "createIndex")
	}

	// Validate each field
	for i, field := range request.Fields {
		if strings.TrimSpace(field.JsonKey) == "" {
			return NewOrisunException(fmt.Sprintf("Field at index %d is missing json_key", i)).
				AddContext("operation", "createIndex")
		}
	}

	return nil
}

// ValidateDropIndexRequest validates a DropIndexRequest
func (v *RequestValidator) ValidateDropIndexRequest(request *eventstore.DropIndexRequest) error {
	if request == nil {
		return NewOrisunException("DropIndexRequest cannot be nil").
			AddContext("operation", "dropIndex")
	}

	// Validate boundary
	if strings.TrimSpace(request.Boundary) == "" {
		return NewOrisunException("Boundary is required").
			AddContext("operation", "dropIndex")
	}

	// Validate name
	if strings.TrimSpace(request.Name) == "" {
		return NewOrisunException("Index name is required").
			AddContext("operation", "dropIndex")
	}

	return nil
}

// ExtractVersionNumbers extracts expected and actual version numbers from an error message
func ExtractVersionNumbers(errorMsg string) (expected, actual int64, err error) {
	// Define the regex pattern to match "Expected X, Actual Y"
	pattern := regexp.MustCompile(`Expected\s+(\d+),\s+Actual\s+(\d+)`)

	matches := pattern.FindStringSubmatch(errorMsg)
	if len(matches) != 3 {
		return 0, 0, fmt.Errorf("could not extract version numbers from error message: %s", errorMsg)
	}

	_, err = fmt.Sscanf(matches[1], "%d", &expected)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse expected version: %w", err)
	}

	_, err = fmt.Sscanf(matches[2], "%d", &actual)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse actual version: %w", err)
	}

	return expected, actual, nil
}
