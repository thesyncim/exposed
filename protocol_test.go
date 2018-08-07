package exposed

import "testing"

func TestSetProtocol(t *testing.T) {

	var h = reqHeader(make([]byte, 9))

	h.SetRequestID(1)
	h.SetStreamID(1)

	if h.RequestUint32ID() != 1 {
		t.Fatal("fatal")
	}
}
