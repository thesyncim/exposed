package exposed

import (
	"bufio"
	"fmt"
	"sync"
)

// response is a TLV response.
type response struct {
	payload []byte
	error   []byte
	sizeBuf [4]byte
}

// Reset resets the given response.
func (resp *response) Reset() {
	resp.payload = resp.payload[:0]
	resp.error = resp.error[:0]
}

// Write appends p to the response payload.
//
// It implements io.Writer.
func (resp *response) Write(p []byte) (int, error) {
	resp.AppendPayload(p)
	return len(p), nil
}

// AppendPayload appends p to the response payload.
func (resp *response) AppendPayload(p []byte) {
	resp.payload = append(resp.payload, p...)
}

// SwapPayload swaps the given payload with the response's payload.
//
// It is forbidden accessing the swapped payload after the call.
func (resp *response) SwapPayload(value []byte) {
	resp.payload = value
	return
}

// SwapError swaps the given payload with the response's payload.
//
// It is forbidden accessing the swapped payload after the call.
func (resp *response) SwapError(value []byte) []byte {
	v := resp.error
	resp.error = value
	return v
}

// Payload returns response payload.
//
// The returned payload is valid until the next response method call.
// or until ReleaseResponse is called.
func (resp *response) Payload() []byte {
	return resp.payload
}

// Payload returns response error.
//
// The returned error is valid until the next response method call.
// or until ReleaseResponse is called.
func (resp *response) Error() []byte {
	return resp.error
}

// writeResponse writes the response to bw.
func (resp *response) WriteResponse(bw *bufio.Writer) error {
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
// It implements exposed.ReadResponse.
func (resp *response) ReadResponse(br *bufio.Reader) error {
	var err error
	resp.payload, err = readBytes(br, resp.payload[:0], resp.sizeBuf[:])
	if err != nil {
		return fmt.Errorf("cannot read response payload: %s", err)
	}
	resp.error, err = readBytes(br, resp.error[:0], resp.sizeBuf[:])
	if err != nil {
		return fmt.Errorf("cannot read response error: %s", err)
	}
	return nil
}

// AcquireResponse acquires new response.
func AcquireResponse() *response {
	v := responsePool.Get()
	if v == nil {
		v = &response{}
	}
	return v.(*response)
}

// ReleaseResponse releases the given response.
func ReleaseResponse(resp *response) {
	resp.Reset()
	responsePool.Put(resp)
}

var responsePool sync.Pool
