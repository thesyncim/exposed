package exposed

import (
	"errors"
	"github.com/thesyncim/exposed/encoding"
	"log"
	"sync/atomic"
)

type StreamServer struct {
	ID       uint32
	isServer bool

	codec encoding.Codec

	inMessages chan *request
	errOutCh   chan error

	streamOutMessage chan *serverStreamOut
	serverOutMessage chan<- *serverStreamOut
	inClosed         uint32
	outClosed        uint32
}

func (sc *StreamServer) sendMessage(m *response, err chan error) {
	if atomic.LoadUint32(&sc.outClosed) == 0 {
		so := acquireServerStreamOut()
		so.streamID = sc.ID
		so.response = m
		so.error = err
		sc.streamOutMessage <- so
	}
}

func NewServerStream(id uint32, codec encoding.Codec, inMessages chan *request, outMessages chan *serverStreamOut) *StreamServer {
	r := &StreamServer{
		ID:               id,
		codec:            codec,
		inMessages:       inMessages,
		serverOutMessage: outMessages,
		streamOutMessage: make(chan *serverStreamOut, 100),
		errOutCh:         make(chan error, 1),
	}
	go r.outWorker()
	return r
}

func (sc *StreamServer) Close() {
	atomic.StoreUint32(&sc.inClosed, 1)
	atomic.StoreUint32(&sc.outClosed, 1)
}

func (sc *StreamServer) outWorker() {
	var m *serverStreamOut
	var ok bool
	for {
		select {
		case m, ok = <-sc.streamOutMessage:
			if ok {
				sc.serverOutMessage <- m
			} else {
				goto quit
			}
		default:
			m, ok = <-sc.streamOutMessage
			if ok {

				sc.serverOutMessage <- m
			} else {

				goto quit
			}

		}
	}
quit:
	log.Println(" StreamServer streamOutMessage closed")
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

	op uint64

	codec encoding.Codec

	inMessages        <-chan *response
	serverOutMessages chan<- *clientStreamOut
	streamOutMessages chan *clientStreamOut

	errOutCh chan error

	inClosed  uint32
	outClosed uint32
}

func NewStreamClient(id uint32, op uint64, codec encoding.Codec, inMessages <-chan *response, outMessages chan<- *clientStreamOut) *StreamClient {
	cs := &StreamClient{
		ID:                id,
		op:                op,
		codec:             codec,
		inMessages:        inMessages,
		serverOutMessages: outMessages,
		streamOutMessages: make(chan *clientStreamOut, 1000),
	}
	go cs.outWorker()
	return cs
}

func (sc *StreamClient) sendMessage(m *request, resp chan error) {
	if atomic.LoadUint32(&sc.outClosed) == 0 {
		sc.streamOutMessages <- &clientStreamOut{
			streamID: sc.ID,
			request:  m,
			error:    resp,
		}
	}

}

func (sc *StreamClient) outWorker() {
	var m *clientStreamOut
	var ok bool
	for {
		select {
		case m, ok = <-sc.streamOutMessages:
			if ok {
				sc.serverOutMessages <- m
			} else {
				goto quit
			}
		default:
			m, ok = <-sc.streamOutMessages
			if ok {
				sc.serverOutMessages <- m
			} else {
				goto quit
			}

		}
	}
quit:
	log.Println("exiting client out worker")

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
