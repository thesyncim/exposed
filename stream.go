package exposed

import (
	"errors"
	"github.com/thesyncim/exposed/encoding"
	"sync/atomic"
)

type StreamServer struct {
	ID       uint32
	isServer bool

	codec encoding.Codec

	inMessages chan *request
	errOutCh   chan error

	stopCh <-chan struct{}

	serverOutMessage chan<- WorkItem
	inClosed         uint32
	outClosed        uint32
}

func (sc *StreamServer) sendMessage(m *response, err chan error) {
	if atomic.LoadUint32(&sc.outClosed) == 0 {
		so := acquireServerStreamOut()
		so.streamID = sc.ID
		so.response = m
		so.error = err
		pushPendingResponse(sc.serverOutMessage, so, sc.stopCh)
	}
}

func NewServerStream(id uint32, codec encoding.Codec, inMessages chan *request, outMessages chan<- WorkItem, stopCh <-chan struct{}) *StreamServer {
	r := &StreamServer{
		ID:               id,
		codec:            codec,
		inMessages:       inMessages,
		serverOutMessage: outMessages,
		errOutCh:         make(chan error, 1),
		stopCh:           stopCh,
	}
	return r
}

func (sc *StreamServer) Close() {
	atomic.StoreUint32(&sc.inClosed, 1)
	atomic.StoreUint32(&sc.outClosed, 1)
}

func (sc *StreamServer) SendMsg(m Message) error {
	if atomic.LoadUint32(&sc.outClosed) == 1 {
		return errClosedWriteStream
	}
	v, err := sc.codec.Marshal(m)
	if err != nil {

		return err
	}
	resp := AcquireResponse()
	resp.SwapPayload(v)
	errch := make(chan error, 1)
	sc.sendMessage(resp, errch)
	return <-errch
}

func (sc *StreamServer) RecvMsg(m Message) (err error) {
	if atomic.LoadUint32(&sc.inClosed) == 1 {
		return errClosedReadStream
	}
	//todo timout
	var mr *request
	var open bool
	if mr, open = <-sc.inMessages; !open {
		return errClosedReadChannel
	}
	err = sc.codec.Unmarshal(mr.payload, m)
	releaseRequest(mr)
	return
}

type StreamClient struct {
	ID       uint32
	isServer bool
	*Client

	op uint64

	codec encoding.Codec

	inMessages        <-chan *response
	serverOutMessages chan<- WorkItem

	errOutCh chan error

	inClosed  uint32
	outClosed uint32
}

func NewStreamClient(c *Client, id uint32, op uint64, codec encoding.Codec, inMessages <-chan *response) *StreamClient {
	cs := &StreamClient{
		Client:     c,
		ID:         id,
		op:         op,
		codec:      codec,
		inMessages: inMessages,
	}
	return cs
}

func (sc *StreamClient) sendMessage(m *request, resp chan error) {
	if atomic.LoadUint32(&sc.outClosed) == 0 {

		err := (sc.enqueueWorkItem(&clientStreamMessageWorkItem{
			streamID: sc.ID,
			request:  m,
			error:    resp,
		}))
		if err != nil {
			resp <- err
		}

	}

}

func (sc *StreamClient) SendMsg(m Message) error {
	if atomic.LoadUint32(&sc.outClosed) == 1 {
		return errClosedWriteStream
	}
	v, err := sc.codec.Marshal(m)
	if err != nil {
		return err
	}
	req := acquireRequest()
	req.SetOperation(sc.op)
	req.SwapPayload(v)
	errch := make(chan error, 1)
	sc.sendMessage(req, errch)
	return <-errch
}

func (sc *StreamClient) RecvMsg(m Message) (err error) {
	if atomic.LoadUint32(&sc.inClosed) == 1 {
		return errClosedReadStream
	}
	//todo timout
	var mr *response
	var ok bool
	if mr, ok = <-sc.inMessages; !ok {
		return errClosedReadChannel
	}

	err = sc.codec.Unmarshal(mr.payload, m)
	ReleaseResponse(mr)
	return err
}

var (
	errClosedReadStream  = errors.New("closed read StreamServer")
	errClosedReadChannel = errors.New("closed read StreamServer channel")
	errClosedWriteStream = errors.New("closed write StreamServer")
)
