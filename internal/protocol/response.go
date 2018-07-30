package protocol

import (
	"bufio"
	"fmt"
	"sync"
)

// Response is a TLV response.
type Response struct {
	payload []byte
	error   []byte
	sizeBuf [4]byte
}

// Reset resets the given response.
func (resp *Response) Reset() {
	resp.payload = resp.payload[:0]
	resp.error = resp.error[:0]
}

// Write appends p to the response payload.
//
// It implements io.Writer.
func (resp *Response) Write(p []byte) (int, error) {
	resp.AppendPayload(p)
	return len(p), nil
}

// AppendPayload appends p to the response payload.
func (resp *Response) AppendPayload(p []byte) {
	resp.payload = append(resp.payload, p...)
}

// SwapPayload swaps the given payload with the response's payload.
//
// It is forbidden accessing the swapped payload after the call.
func (resp *Response) SwapPayload(value []byte) []byte {
	v := resp.payload
	resp.payload = value
	return v
}

// SwapError swaps the given payload with the response's payload.
//
// It is forbidden accessing the swapped payload after the call.
func (resp *Response) SwapError(value []byte) []byte {
	v := resp.error
	resp.error = value
	return v
}

// Payload returns response payload.
//
// The returned payload is valid until the next Response method call.
// or until ReleaseResponse is called.
func (resp *Response) Payload() []byte {
	return resp.payload
}

// Payload returns response error.
//
// The returned error is valid until the next Response method call.
// or until ReleaseResponse is called.
func (resp *Response) Error() []byte {
	return resp.error
}

// writeResponse writes the response to bw.
func (resp *Response) WriteResponse(bw *bufio.Writer) error {
	if err := writeBytes(bw, resp.payload, resp.sizeBuf[:]); err != nil {
		return fmt.Errorf("cannot write response payload: %s", err)
	}
	if err := writeBytes(bw, resp.error, resp.sizeBuf[:]); err != nil {
		return fmt.Errorf("cannot write response error: %s", err)
	}
	return nil
}

// ReadResponse reads the response from br.
//
// It implements fastrpc.ReadResponse.
func (resp *Response) ReadResponse(br *bufio.Reader) error {
	var err error
	resp.payload, err = readBytes(br, resp.payload[:0], resp.sizeBuf[:])
	if err != nil {
		return fmt.Errorf("cannot read request payload: %s", err)
	}
	resp.error, err = readBytes(br, resp.error[:0], resp.sizeBuf[:])
	if err != nil {
		return fmt.Errorf("cannot read request error: %s", err)
	}
	return nil
}

// AcquireResponse acquires new response.
func AcquireResponse() *Response {
	v := responsePool.Get()
	if v == nil {
		v = &Response{}
	}
	return v.(*Response)
}

// ReleaseResponse releases the given response.
func ReleaseResponse(resp *Response) {
	resp.Reset()
	responsePool.Put(resp)
}

var responsePool sync.Pool
