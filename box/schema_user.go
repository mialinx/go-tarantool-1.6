package box

import (
	"context"
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// SchemaUser provides methods to interact with schema-related user operations in Tarantool.
type SchemaUser struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// NewSchemaUser creates a new SchemaUser instance with the provided Tarantool connection.
// It initializes a SchemaUser object, which provides methods to perform user-related
// schema operations (such as creating, modifying, or deleting users) in the Tarantool instance.
func NewSchemaUser(conn tarantool.Doer) *SchemaUser {
	return &SchemaUser{conn: conn}
}

// UserExistsRequest represents a request to check if a user exists in Tarantool.
type UserExistsRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// UserExistsResponse represents the response to a user existence check.
type UserExistsResponse struct {
	Exists bool // True if the user exists, false otherwise.
}

// DecodeMsgpack decodes the response from a Msgpack-encoded byte slice.
func (uer *UserExistsResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	// Ensure that the response array contains exactly 1 element (the "Exists" field).
	if arrayLen != 1 {
		return fmt.Errorf("protocol violation; expected 1 array entry, got %d", arrayLen)
	}

	// Decode the boolean value indicating whether the user exists.
	uer.Exists, err = d.DecodeBool()

	return err
}

// NewUserExistsRequest creates a new request to check if a user exists.
func NewUserExistsRequest(username string) UserExistsRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.exists").Args([]interface{}{username})

	return UserExistsRequest{
		callReq,
	}
}

// Exists checks if the specified user exists in Tarantool.
func (u *SchemaUser) Exists(ctx context.Context, username string) (bool, error) {
	// Create a request and send it to Tarantool.
	req := NewUserExistsRequest(username).Context(ctx)
	resp := &UserExistsResponse{}

	// Execute the request and parse the response.
	err := u.conn.Do(req).GetTyped(resp)

	return resp.Exists, err
}

// UserCreateOptions represents options for creating a user in Tarantool.
type UserCreateOptions struct {
	// IfNotExists - if true, prevents an error if the user already exists.
	IfNotExists bool `msgpack:"if_not_exists"`
	// Password for the new user.
	Password string `msgpack:"password"`
}

// UserCreateRequest represents a request to create a new user in Tarantool.
type UserCreateRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserCreateRequest creates a new request to create a user with specified options.
func NewUserCreateRequest(username string, options UserCreateOptions) UserCreateRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.create").
		Args([]interface{}{username, options})

	return UserCreateRequest{
		callReq,
	}
}

// UserCreateResponse represents the response to a user creation request.
type UserCreateResponse struct{}

// DecodeMsgpack decodes the response for a user creation request.
// In this case, the response does not contain any data.
func (uer *UserCreateResponse) DecodeMsgpack(_ *msgpack.Decoder) error {
	return nil
}

// Create creates a new user in Tarantool with the given username and options.
func (u *SchemaUser) Create(ctx context.Context, username string, options UserCreateOptions) error {
	// Create a request and send it to Tarantool.
	req := NewUserCreateRequest(username, options).Context(ctx)
	resp := &UserCreateResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	err := fut.GetTyped(resp)
	if err != nil {
		return err
	}

	return nil
}

// UserDropOptions represents options for dropping a user in Tarantool.
type UserDropOptions struct {
	IfExists bool `msgpack:"if_exists"` // If true, prevents an error if the user does not exist.
}

// UserDropRequest represents a request to drop a user from Tarantool.
type UserDropRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserDropRequest creates a new request to drop a user with specified options.
func NewUserDropRequest(username string, options UserDropOptions) UserDropRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.drop").
		Args([]interface{}{username, options})

	return UserDropRequest{
		callReq,
	}
}

// UserDropResponse represents the response to a user drop request.
type UserDropResponse struct{}

// Drop drops the specified user from Tarantool, with optional conditions.
func (u *SchemaUser) Drop(ctx context.Context, username string, options UserDropOptions) error {
	// Create a request and send it to Tarantool.
	req := NewUserDropRequest(username, options).Context(ctx)
	resp := &UserCreateResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	err := fut.GetTyped(resp)
	if err != nil {
		return err
	}

	return nil
}

// UserPasswordRequest represents a request to retrieve a user's password from Tarantool.
type UserPasswordRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserPasswordRequest creates a new request to fetch the user's password.
// It takes the username and constructs the request to Tarantool.
func NewUserPasswordRequest(username string) UserPasswordRequest {
	// Create a request to get the user's password.
	callReq := tarantool.NewCallRequest("box.schema.user.password").Args([]interface{}{username})

	return UserPasswordRequest{
		callReq,
	}
}

// UserPasswordResponse represents the response to the user password request.
// It contains the password hash.
type UserPasswordResponse struct {
	Hash string // The password hash of the user.
}

// DecodeMsgpack decodes the response from Tarantool in Msgpack format.
// It expects the response to be an array of length 1, containing the password hash string.
func (upr *UserPasswordResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	// Decode the array length.
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	// Ensure the array contains exactly 1 element (the password hash).
	if arrayLen != 1 {
		return fmt.Errorf("protocol violation; expected 1 array entry, got %d", arrayLen)
	}

	// Decode the string containing the password hash.
	upr.Hash, err = d.DecodeString()

	return err
}

// Password sends a request to retrieve the user's password from Tarantool.
// It returns the password hash as a string or an error if the request fails.
func (u *SchemaUser) Password(ctx context.Context, username string) (string, error) {
	// Create the request and send it to Tarantool.
	req := NewUserPasswordRequest(username).Context(ctx)
	resp := &UserPasswordResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	// Get the decoded response.
	err := fut.GetTyped(resp)
	if err != nil {
		return "", err
	}

	// Return the password hash.
	return resp.Hash, nil
}
