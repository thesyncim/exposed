package exposed

import (
	"errors"
)

var (
	errTransport                  = errors.New("transport error")
	errOperation                  = errors.New("operation error")
	errHandlerNotFound            = errors.New("handler not found")
	errOperationAlreadyRegistered = errors.New("type already registered")
)

//Exposable interface autoregister services Operations
type Exposable interface {
	//ExposedOperations return a list of operations
	ExposedOperations() []OperationInfo
}

//Message represents an interchangeable payload
type Message = interface{}

type Handler interface {
	ServeExposed(ctx *Context, req Message, resp Message) (err error)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as exposed RPC handlers.
type HandlerFunc func(ctx *Context, req Message, resp Message) (err error)

func (h HandlerFunc) ServeExposed(ctx *Context, req Message, resp Message) (err error) {
	return h(ctx, req, resp)
}
