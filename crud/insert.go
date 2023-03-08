package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// InsertResult describes result for `crud.insert` method.
type InsertResult = Result

// InsertOpts describes options for `crud.insert` method.
type InsertOpts = SimpleOperationOpts

// InsertRequest helps you to create request object to call `crud.insert`
// for execution by a Connection.
type InsertRequest struct {
	spaceRequest
	tuple Tuple
	opts  InsertOpts
}

type insertArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Tuple    Tuple
	Opts     InsertOpts
}

// NewInsertRequest returns a new empty InsertRequest.
func NewInsertRequest(space string) *InsertRequest {
	req := new(InsertRequest)
	req.initImpl("crud.insert")
	req.setSpace(space)
	req.tuple = Tuple{}
	req.opts = InsertOpts{}
	return req
}

// Tuple sets the tuple for the InsertRequest request.
// Note: default value is nil.
func (req *InsertRequest) Tuple(tuple Tuple) *InsertRequest {
	req.tuple = tuple
	return req
}

// Opts sets the options for the insert request.
// Note: default value is nil.
func (req *InsertRequest) Opts(opts InsertOpts) *InsertRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *InsertRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := insertArgs{Space: req.space, Tuple: req.tuple, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *InsertRequest) Context(ctx context.Context) *InsertRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// InsertObjectResult describes result for `crud.insert_object` method.
type InsertObjectResult = Result

// InsertObjectOpts describes options for `crud.insert_object` method.
type InsertObjectOpts = SimpleOperationObjectOpts

// InsertObjectRequest helps you to create request object to call
// `crud.insert_object` for execution by a Connection.
type InsertObjectRequest struct {
	spaceRequest
	object Object
	opts   InsertObjectOpts
}

type insertObjectArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Object   Object
	Opts     InsertObjectOpts
}

// NewInsertObjectRequest returns a new empty InsertObjectRequest.
func NewInsertObjectRequest(space string) *InsertObjectRequest {
	req := new(InsertObjectRequest)
	req.initImpl("crud.insert_object")
	req.setSpace(space)
	req.object = MapObject{}
	req.opts = InsertObjectOpts{}
	return req
}

// Object sets the tuple for the InsertObjectRequest request.
// Note: default value is nil.
func (req *InsertObjectRequest) Object(object Object) *InsertObjectRequest {
	req.object = object
	return req
}

// Opts sets the options for the InsertObjectRequest request.
// Note: default value is nil.
func (req *InsertObjectRequest) Opts(opts InsertObjectOpts) *InsertObjectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *InsertObjectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := insertObjectArgs{Space: req.space, Object: req.object, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *InsertObjectRequest) Context(ctx context.Context) *InsertObjectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
