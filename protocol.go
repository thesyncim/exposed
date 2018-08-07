package exposed

import "encoding/binary"

type packetControl byte

const (
	unary         = packetControl(0)
	streamMessage = packetControl(1)
	streamStart   = packetControl(2)
	///todo implement
	streamClose      = packetControl(3)
	streamCloseRead  = packetControl(4)
	streamCloseWrite = packetControl(5)
)

type respHeader []byte

func (h respHeader) Control() packetControl {
	return packetControl(h[0])
}

func (h *respHeader) SetControl(b packetControl) {
	(*h)[0] = byte(b)
}

func (h respHeader) ID() []byte {
	return h[1:5]
}

func (h respHeader) Uint32ID() uint32 {
	return binary.BigEndian.Uint32(h[1:5])
}

func (h *respHeader) SetID(sid uint32) {
	binary.BigEndian.PutUint32((*h)[1:5], sid)
}

type reqHeader []byte

func (h reqHeader) Control() packetControl {
	return packetControl(h[0])
}

func (h *reqHeader) SetControl(b packetControl) {
	(*h)[0] = byte(b)
}

func (h reqHeader) StreamID() (sid []byte) {
	return h[1:5]
}

func (h *reqHeader) SetStreamID(sid uint32) {
	binary.BigEndian.PutUint32((*h)[1:5], sid)
}

func (h reqHeader) StreamUint32ID() uint32 {
	return binary.BigEndian.Uint32(h[1:5])
}

func (h *reqHeader) SetRequestID(sid uint32) {
	binary.BigEndian.PutUint32((*h)[5:9], sid)

}

func (h reqHeader) RequestID() (reqid []byte) {
	return h[5:9]
}

func (h reqHeader) RequestUint32ID() uint32 {
	return binary.BigEndian.Uint32(h[5:9])
}
