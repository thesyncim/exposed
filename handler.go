package exposed

import (
	"bufio"
	"fmt"
	"net"

	"github.com/rs/zerolog"
	"github.com/thesyncim/exposed/encoding"
	"github.com/thesyncim/exposed/internal/protocol"
)

// exposedCtx implements HandlerCtx
type exposedCtx struct {
	Request  *protocol.Request
	Response *protocol.Response

	codec encoding.Codec
}

func newExposedCtx(codec encoding.Codec) func() HandlerCtx {
	return func() HandlerCtx {
		return &exposedCtx{
			codec:    codec,
			Response: protocol.AcquireResponse(),
			Request:  protocol.AcquireRequest(),
		}
	}
}

func (h *exposedCtx) Handle(ctxv HandlerCtx) (rctxv HandlerCtx) {

	eh := ctxv.(*exposedCtx)
	defer func() {
		if r := recover(); r != nil {
			eh.Response.SwapError(append([]byte("panic captured: "), []byte(fmt.Sprint(r))...))
			rctxv = ctxv
			return
		}
	}()

	handler, err := match(eh.Request.Operation())
	if err != nil {
		h.Response.SwapError([]byte(err.Error()))
		return ctxv
	}

	opinfo := getOperationInfo(eh.Request.Operation())

	args := opinfo.ArgsType()

	if err := h.codec.Unmarshal(eh.Request.Payload(), args); err != nil {
		panic(err)
	}
	reply := opinfo.ReplyType()

	if err = handler(nil, args, reply); err != nil {
		eh.Response.SwapError([]byte(err.Error()))
		return ctxv
	}

	v, err := h.codec.Marshal(reply)
	if err != nil {
		eh.Response.SwapError([]byte(err.Error()))
		return ctxv
	}

	eh.Response.SwapPayload(v)
	return ctxv
}

func (h *exposedCtx) ConcurrencyLimitError(concurrency int) {
	h.Response.SwapError([]byte("max concurrency excedded"))
}

func (h *exposedCtx) init(conn net.Conn, logger *zerolog.Logger) {
	h.Request.Reset()
	h.Response.Reset()
}

func (h *exposedCtx) ReadRequest(br *bufio.Reader) error {
	return h.Request.ReadRequest(br)
}

func (h *exposedCtx) WriteResponse(bw *bufio.Writer) error {
	return h.Response.WriteResponse(bw)
}
