package exposed

import (
	"bufio"
	"fmt"
	"sync"
)

// request is a TLV request.
type request struct {
	payload   []byte
	operation uint64
	opBuf     [8]byte
	sizeBuf   [4]byte
}

// Reset resets the given request.
func (req *request) Reset() {
	req.operation = 0
	req.payload = req.payload[:0]
}

//SetOperation sets request operation.
func (req *request) SetOperation(op uint64) {
	req.operation = op
}

// Operation returns request operation.
//
// The returned payload is valid until the next request method call
// or until releaseRequest is called.
func (req *request) Operation() uint64 {
	return req.operation
}

// Write appends p to the request payload.
//
// It implements io.Writer.
func (req *request) Write(p []byte) (int, error) {
	req.Append(p)
	return len(p), nil
}

// AppendPayload appends p to the request payload.
func (req *request) Append(p []byte) {
	req.payload = append(req.payload, p...)
}

// SwapPayload swaps the given payload with the request's payload.
//
// It is forbidden accessing the swapped payload after the call.
func (req *request) SwapPayload(value []byte) {
	req.payload = value
	return
}

// Payload returns request payload.
//
// The returned payload is valid until the next request method call.
// or until releaseRequest is called.
func (req *request) Payload() []byte {
	return req.payload
}

// WriteRequest writes the request to bw.
//
// It implements exposed.RequestWriter
func (req *request) WriteRequest(bw *bufio.Writer) error {
	if err := writeOperation(bw, req.operation, req.opBuf[:]); err != nil {
		return fmt.Errorf("cannot write request operation: %s", err)
	}
	if err := writeBytes(bw, req.payload, req.sizeBuf[:]); err != nil {
		return fmt.Errorf("cannot write request payload: %s", err)
	}
	return nil
}

// readRequest reads the request from br.
func (req *request) ReadRequest(br *bufio.Reader) (err error) {

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

// acquireRequest acquires new request.
func acquireRequest() *request {
	v := requestPool.Get()
	if v == nil {
		v = &request{}
	}
	return v.(*request)
}

// releaseRequest releases the given request.
func releaseRequest(req *request) {
	req.Reset()
	requestPool.Put(req)
}

var requestPool sync.Pool
