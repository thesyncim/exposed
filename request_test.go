package exposed

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestRequestMarshalUnmarshal(t *testing.T) {
	var buf bytes.Buffer

	req := AcquireRequest()
	bw := bufio.NewWriter(&buf)
	for i := 0; i < 10; i++ {
		op := uint64(1)
		value := fmt.Sprintf("payload %d", i)
		req.SetOperation(op)
		req.SwapValue([]byte(value))
		if err := req.WriteRequest(bw); err != nil {
			t.Fatalf("unexpected error when writing request: %s", err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("unexpected error when flushing request: %s", err)
	}
	ReleaseRequest(req)

	req1 := AcquireRequest()
	br := bufio.NewReader(&buf)
	for i := 0; i < 10; i++ {
		name := uint64(1)
		value := fmt.Sprintf("payload %d", i)
		if err := req1.ReadRequest(br); err != nil {
			t.Fatalf("unexpected error when reading request: %s", err)
		}
		if req1.Operation() != name {
			t.Fatalf("unexpected request operation read: %q. Expecting %q", req1.Operation(), name)
		}
		if string(req1.Payload()) != value {
			t.Fatalf("unexpected request payload read: %q. Expecting %q", req1.Payload(), value)
		}
	}
	ReleaseRequest(req1)
}
