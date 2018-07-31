package exposed

import (
	"bufio"
	"fmt"
	"sync"
)

// Request is a TLV request.
type Request struct {
	payload   []byte
	operation uint64
	opBuf     [8]byte
	sizeBuf   [4]byte
}

// Reset resets the given request.
func (req *Request) Reset() {
	req.operation = 0
	req.payload = req.payload[:0]
}

//SetOperation sets request operation.
func (req *Request) SetOperation(op uint64) {
	req.operation = op
}

// Operation returns request operation.
//
// The returned payload is valid until the next Request method call
// or until ReleaseRequest is called.
func (req *Request) Operation() uint64 {
	return req.operation
}

// Write appends p to the request payload.
//
// It implements io.Writer.
func (req *Request) Write(p []byte) (int, error) {
	req.Append(p)
	return len(p), nil
}

// AppendPayload appends p to the request payload.
func (req *Request) Append(p []byte) {
	req.payload = append(req.payload, p...)
}

// SwapPayload swaps the given payload with the request's payload.
//
// It is forbidden accessing the swapped payload after the call.
func (req *Request) SwapValue(value []byte) []byte {
	v := req.payload
	req.payload = value
	return v
}

// Payload returns request payload.
//
// The returned payload is valid until the next Request method call.
// or until ReleaseRequest is called.
func (req *Request) Payload() []byte {
	return req.payload
}

// WriteRequest writes the request to bw.
//
// It implements fastrpc.RequestWriter
func (req *Request) WriteRequest(bw *bufio.Writer) error {
	if err := writeOperation(bw, req.operation, req.opBuf[:]); err != nil {
		return fmt.Errorf("cannot write request operation: %s", err)
	}
	if err := writeBytes(bw, req.payload, req.sizeBuf[:]); err != nil {
		return fmt.Errorf("cannot write request payload: %s", err)
	}
	return nil
}

// readRequest reads the request from br.
func (req *Request) ReadRequest(br *bufio.Reader) (err error) {

	req.operation, err = readOperation(br, req.opBuf[:])
	if err != nil {
		return fmt.Errorf("cannot read request operation: %s", err)
	}
	req.payload, err = readBytes(br, req.payload[:0], req.sizeBuf[:])
	if err != nil {
		return fmt.Errorf("cannot read request payload: %s", err)
	}
	return nil
}

// AcquireRequest acquires new request.
func AcquireRequest() *Request {
	v := requestPool.Get()
	if v == nil {
		v = &Request{}
	}
	return v.(*Request)
}

// ReleaseRequest releases the given request.
func ReleaseRequest(req *Request) {
	req.Reset()
	requestPool.Put(req)
}

var requestPool sync.Pool
