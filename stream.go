package exposed

import (
	"errors"
	"github.com/thesyncim/exposed/encoding"
)

type StreamServer struct {
	ID       uint32
	isServer bool

	codec encoding.Codec

	inMessages chan *request
	errOutCh   chan error

	sendOutMessages func(id uint32, m *response, resp chan error)
	inClosed        bool
	outClosed       bool
}

func NewServerStream(id uint32, codec encoding.Codec, inMessages chan *request, sendf func(id uint32, m *response, resp chan error)) *StreamServer {
	return &StreamServer{
		ID:              id,
		codec:           codec,
		inMessages:      inMessages,
		sendOutMessages: sendf,
	}
}

func (sc *StreamServer) SendMsg(m Message) error {
	if sc.outClosed {
		return errClosedWriteStream
	}
	v, err := sc.codec.Marshal(m)
	if err != nil {

		return err
	}
	resp := AcquireResponse()
	resp.SwapPayload(v)
	errch := make(chan error, 1)
	sc.sendOutMessages(sc.ID, resp, errch)
	return <-errch
}

func (sc *StreamServer) RecvMsg(m Message) (err error) {
	if sc.inClosed {
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

	op uint64

	codec encoding.Codec

	inMessages <-chan *response
	errOutCh   chan error

	sendOutMessages func(id uint32, m *request, resp chan error)
	inClosed        bool
	outClosed       bool
}

func NewStreamClient(id uint32, op uint64, codec encoding.Codec, inMessages <-chan *response, sendf func(id uint32, m *request, resp chan error)) *StreamClient {
	return &StreamClient{
		ID:              id,
		op:              op,
		codec:           codec,
		inMessages:      inMessages,
		sendOutMessages: sendf,
	}
}

func (sc *StreamClient) SendMsg(m Message) error {
	if sc.outClosed {
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
	sc.sendOutMessages(sc.ID, req, errch)
	return <-errch
}

func (sc *StreamClient) RecvMsg(m Message) (err error) {
	if sc.inClosed {
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
