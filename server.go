package exposed

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/binary"
	"github.com/rs/zerolog"
	"github.com/thesyncim/exposed/encoding"
	"github.com/thesyncim/exposed/encoding/codec/proto"
	"golang.org/x/net/trace"
)

// A ServerOption sets options such as codec, compress type, etc.
type ServerOption func(*serverOptions)

type serverOptions struct {
	// CompressType is the compression type used for responses.
	//
	// CompressFlate is used by default.
	CompressType CompressType

	// Concurrency is the maximum number of concurrent goroutines
	// with Server.Handler the server may run.
	//
	// DefaultConcurrency is used by default.
	Concurrency uint32

	// TLSConfig is TLS (aka SSL) config used for accepting encrypted
	// client connections.
	//
	// Encrypted connections may be used for transferring sensitive
	// information over untrusted networks.
	//
	// By default server accepts only unencrypted connections.
	TLSConfig *tls.Config

	// MaxBatchDelay is the maximum duration before ready responses
	// are sent to the client.
	//
	// Responses' batching may reduce network bandwidth usage and CPU usage.
	//
	// By default responses are sent immediately to the client.
	MaxBatchDelay time.Duration

	// Maximum duration for reading the full request (including body).
	////87	// This also limits the maximum lifetime for idle connections.
	//
	// By default request read timeout is unlimited.
	ReadTimeout time.Duration

	// Maximum duration for writing the full response (including body).
	//
	// By default response write timeout is unlimited.
	WriteTimeout time.Duration

	// ReadBufferSize is the size for read buffer.
	//
	// DefaultReadBufferSize is used by default.
	ReadBufferSize int

	// WriteBufferSize is the size for write buffer.
	//
	// DefaultWriteBufferSize is used by default.
	WriteBufferSize int

	//Codec
	Codec encoding.Codec
}

var defaultServerOptions = serverOptions{
	Codec:           encoding.GetCodec(proto.CodecName),
	TLSConfig:       nil,
	CompressType:    CompressNone,
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	Concurrency:     100000,
	ReadTimeout:     time.Second * 30,
	WriteTimeout:    time.Second * 30,
	MaxBatchDelay:   0,
}

// Server accepts rpc requests from Client.
type Server struct {
	opts serverOptions

	// SniffHeader is the header read from each client connection.
	//
	// The server sends the same header to each client.
	SniffHeader string

	// ProtocolVersion is the version of *exposedCtx.ReadRequest
	// and *exposedCtx.WriteResponse.
	//
	// The ProtocolVersion must be changed each time *exposedCtx.ReadRequest
	// or *exposedCtx.WriteResponse changes the underlying format.
	ProtocolVersion byte

	// NewHandlerCtx must return new *exposedCtx
	NewHandlerCtx func() *exposedCtx

	// Handler must process incoming requests.
	//
	// The handler must return either ctx passed to the call
	// or new non-nil ctx.
	//
	// The handler may return ctx passed to the call only if the ctx
	// is no longer used after returning from the handler.
	// Otherwise new ctx must be returned.
	Handler func(ctx *exposedCtx, stream Stream) *exposedCtx

	// Logger, which is used by the Server.
	//
	// Standard logger from log package is used by default.
	Logger *zerolog.Logger

	events     trace.EventLog
	eventsLock sync.Mutex

	outStreamMessages chan *serverStreamOut

	workItemPool sync.Pool

	concurrencyCount uint32
}

// NewServer creates a exposed server with the provided server options which has no service registered and has not
// started to accept requests yet.
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		ProtocolVersion: byte(2),
		SniffHeader:     "exposed",
	}
	s.opts = defaultServerOptions
	for _, o := range opts {
		o(&s.opts)
	}
	eHandler := newExposedCtx(s.opts.Codec)().Handle
	s.Handler = eHandler
	//s.inStreamMsg = map[uint32]chan *request{}
	s.outStreamMessages = make(chan *serverStreamOut, 10000)
	s.NewHandlerCtx = newExposedCtx(s.opts.Codec)

	return s
}

func (s *Server) concurrency() int {

	return int(s.opts.Concurrency)
}

// Serve serves rpc requests accepted from the given listener.
func (s *Server) Serve(ln net.Listener) error {
	if s.Handler == nil {
		panic("BUG: Server.Handler must be set")
	}
	concurrency := s.concurrency()
	//pipelineRequests := s.opts.PipelineRequests
	for {
		conn, err := ln.Accept()
		if err != nil {
			if conn != nil {
				panic("BUG: net.Listener returned non-nil conn and non-nil error")
			}
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				s.logger().Printf("exposed.Server: temporary error when accepting new connections: %s", netErr)
				time.Sleep(time.Second)
				continue
			}
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				s.logger().Printf("exposed.Server: permanent error when accepting new connections: %s", err)
				return err
			}
			return nil
		}
		if conn == nil {
			panic("BUG: net.Listener returned (nil, nil)")
		}

		n := int(atomic.LoadUint32(&s.concurrencyCount))
		if n > concurrency {

			s.logger().Printf("exposed.Server: concurrency limit exceeded: %d", concurrency)
			continue
		}

		go func() {
			laddr := conn.LocalAddr().String()
			raddr := conn.RemoteAddr().String()
			if err := s.serveConn(conn); err != nil {
				s.logger().Printf("exposed.Server: error on connection %q<->%q: %s", laddr, raddr, err)
			}
			/*	if pipelineRequests {
					atomic.AddUint32(&s.concurrencyCount, ^uint32(0))
				}
			*/
		}()
	}
}

type serverStreamOut struct {
	streamID uint32
	sobuf    [4]byte
	*response
	error chan error
}

var stwpool sync.Pool

func acquireServerStreamOut() *serverStreamOut {
	v := stwpool.Get()
	if v == nil {
		return &serverStreamOut{}
	}

	return v.(*serverStreamOut)
}

func releaseServerStreamOut(sso *serverStreamOut) {
	stwpool.Put(sso)
}
func (s *Server) serveConn(conn net.Conn) error {

	cfg := &handshakeConfig{
		sniffHeader:       []byte(s.SniffHeader),
		protocolVersion:   s.ProtocolVersion,
		conn:              conn,
		readBufferSize:    s.opts.ReadBufferSize,
		writeBufferSize:   s.opts.WriteBufferSize,
		writeCompressType: s.opts.CompressType,
		tlsConfig:         s.opts.TLSConfig,
		isServer:          true,
	}
	br, bw, pipelineRequests, err := newBufioConn(cfg)
	if err != nil {
		conn.Close()
		return err
	}

	stopCh := make(chan struct{})

	//type map[uint32]chan *request
	var inStreamMsg sync.Map

	pendingResponses := make(chan *serverWorkItem, s.concurrency())
	readerDone := make(chan error, 1)
	go func() {
		readerDone <- s.connReader(br, &inStreamMsg, pipelineRequests, conn, pendingResponses, stopCh)
	}()

	writerDone := make(chan error, 1)

	go func() {
		writerDone <- s.connWriter(bw, conn, pendingResponses, s.outStreamMessages, stopCh)
	}()

	select {
	case err = <-readerDone:
		conn.Close()
		close(stopCh)
		inStreamMsg.Range(func(key, value interface{}) bool {
			panic("das")
			close(value.(chan *request))
			return true
		})
		<-writerDone
	case err = <-writerDone:
		conn.Close()
		inStreamMsg.Range(func(key, value interface{}) bool {
			panic("das")
			close(value.(chan *request))
			return true
		})
		close(stopCh)
		<-readerDone
	}

	return err
}

func (s *Server) connReader(br *bufio.Reader, inStreamMsg *sync.Map, pipeline bool, conn net.Conn, pendingResponses chan<- *serverWorkItem, stopCh <-chan struct{}) error {
	logger := s.logger()
	concurrency := s.concurrency()
	pipelineRequests := pipeline
	readTimeout := s.opts.ReadTimeout
	var header = reqHeader(make([]byte, 9))
	var lastReadDeadline time.Time
	for {
		wi := s.acquireWorkItem()

		if readTimeout > 0 {
			// Optimization: update read deadline only if more than 25%
			// of the last read deadline exceeded.
			// See https://github.com/golang/go/issues/15133 for details.
			t := coarseTimeNow()
			if t.Sub(lastReadDeadline) > (readTimeout >> 2) {
				if err := conn.SetReadDeadline(t.Add(readTimeout)); err != nil {
					// do not panic here, since the error may
					// indicate that the connection is already closed
					//wi.event.Errorf("cannot update read deadline: %s", err)
					return fmt.Errorf("cannot update read deadline: %s", err)
				}
				lastReadDeadline = t
			}
		}

		if n, err := io.ReadFull(br, header[:]); err != nil {
			if n == 0 {
				// Ignore error if no bytes are read, since
				// the client may just close the connection.
				return nil
			}
			//wi.event.Errorf("cannot read request ID: %s", err)

			return fmt.Errorf("cannot read streamCrtlBuf ID: %s", err)
		}

		switch header.Control() {

		case streamMessage:
			req := acquireRequest()
			//the stream exists
			if inStream, ok := inStreamMsg.Load(header.StreamUint32ID()); ok {
				if err := req.ReadRequest(br); err != nil {
					//	wi.event.Errorf("cannot read request: %s", err)
					close(inStream.(chan *request))
					return fmt.Errorf("cannot read request: %s", err)
				}
				inStream.(chan *request) <- req

			} else { //create the stream handler
				panic("stream doesnt exists")

			}
		case streamStart:
			copy(wi.streamID[:], header.StreamID())
			copy(wi.reqID[:], header.RequestID())
			wi.ctx.Init(conn, logger)
			wi.startStream = true
			if err := wi.ctx.ReadRequest(br); err != nil {
				//	wi.event.Errorf("cannot read request: %s", err)
				return fmt.Errorf("cannot read request: %s", err)
			}

			n := int(atomic.AddUint32(&s.concurrencyCount, 1))
			if n > concurrency {
				atomic.AddUint32(&s.concurrencyCount, ^uint32(0))
				wi.ctx.ConcurrencyLimitError(concurrency)
				if !pushPendingResponse(pendingResponses, wi, stopCh) {
					return nil
				}
				continue
			}

			var stream *StreamServer
			if wi.startStream {
				in, _ := inStreamMsg.LoadOrStore(header.StreamUint32ID(), make(chan *request, 1000))
				stream = NewServerStream(header.StreamUint32ID(), s.opts.Codec, in.(chan *request), s.outStreamMessages)
			}
			go func(wi *serverWorkItem) {
				s.handleRequest(wi, pendingResponses, stream, stopCh)
				atomic.AddUint32(&s.concurrencyCount, ^uint32(0))
			}(wi)

		case unary:
			copy(wi.streamID[:], header.StreamID())
			copy(wi.reqID[:], header.RequestID())
			wi.ctx.Init(conn, logger)
			if err := wi.ctx.ReadRequest(br); err != nil {
				//	wi.event.Errorf("cannot read request: %s", err)
				return fmt.Errorf("cannot read request: %s", err)
			}

			if pipelineRequests {
				atomic.AddUint32(&s.concurrencyCount, 1)
				s.handleRequest(wi, pendingResponses, nil, stopCh)
				atomic.AddUint32(&s.concurrencyCount, ^uint32(0))
			} else {
				n := int(atomic.AddUint32(&s.concurrencyCount, 1))
				if n > concurrency {
					atomic.AddUint32(&s.concurrencyCount, ^uint32(0))
					wi.ctx.ConcurrencyLimitError(concurrency)
					if !pushPendingResponse(pendingResponses, wi, stopCh) {
						return nil
					}
					continue
				}
				go func(wi *serverWorkItem) {
					s.handleRequest(wi, pendingResponses, nil, stopCh)
					atomic.AddUint32(&s.concurrencyCount, ^uint32(0))
				}(wi)
			}

		}

	}
}

func (s *Server) handleRequest(wi *serverWorkItem, pendingResponses chan<- *serverWorkItem, stream *StreamServer, stopCh <-chan struct{}) {
	reqID := wi.reqID
	//wi.event.Printf("start handling request id %v", binary.BigEndian.Uint32(reqID[:]))

	ctxNew := s.Handler(wi.ctx, stream)
	//wi.event.Printf("finish handling request id %v", binary.BigEndian.Uint32(reqID[:]))
	if stream != nil {
		close(stream.streamOutMessage)

	}

	if isZeroReqID(reqID) {
		// Do not send response for SendNowait request.
		if ctxNew == wi.ctx {
			s.releaseWorkItem(wi)
		}
		return
	}

	//
	if ctxNew != wi.ctx {
		if ctxNew == nil {
			panic("BUG: Server.Handler mustn't return nil")
		}
		// The current ctx may be still in use by the handler.
		// So create new wi for passing to pendingResponses.
		wi = s.acquireWorkItem()
		wi.reqID = reqID
		wi.ctx = ctxNew
	}
	pushPendingResponse(pendingResponses, wi, stopCh)

}

func (s *Server) RegisterService(e Exposable) {
	registerService(e)
}

func (s *Server) HandleFunc(path string, handlerFunc HandlerFunc, info *OperationTypes) {
	registerHandleFunc(path, handlerFunc, info)
}

func pushPendingResponse(pendingResponses chan<- *serverWorkItem, wi *serverWorkItem, stopCh <-chan struct{}) bool {
	select {
	case pendingResponses <- wi:
	default:
		select {
		case pendingResponses <- wi:
		case <-stopCh:
			return false
		}
	}
	return true
}

func (s *Server) connWriter(bw *bufio.Writer, conn net.Conn, pendingResponses <-chan *serverWorkItem, streamOut <-chan *serverStreamOut, stopCh <-chan struct{}) error {
	var wi *serverWorkItem
	var so *serverStreamOut

	var (
		flushTimer    = getFlushTimer()
		flushCh       <-chan time.Time
		flushAlwaysCh = make(chan time.Time)

		header = respHeader(make([]byte, 5))
	)
	defer putFlushTimer(flushTimer)

	close(flushAlwaysCh)
	maxBatchDelay := s.opts.MaxBatchDelay
	if maxBatchDelay < 0 {
		maxBatchDelay = 0
	}

	writeTimeout := s.opts.WriteTimeout
	var lastWriteDeadline time.Time
	for {
		select {
		case wi = <-pendingResponses:
		case so = <-streamOut:
		default:
			select {
			case wi = <-pendingResponses:
			case so = <-streamOut:
			case <-stopCh:
				return nil
			case <-flushCh:
				if err := bw.Flush(); err != nil {
					return fmt.Errorf("cannot flush response data to client: %s", err)
				}
				flushCh = nil
				continue
			}
		}

		if writeTimeout > 0 {
			// Optimization: update write deadline only if more than 25%
			// of the last write deadline exceeded.
			// See https://github.com/golang/go/issues/15133 for details.
			t := coarseTimeNow()
			if t.Sub(lastWriteDeadline) > (writeTimeout >> 2) {
				if err := conn.SetWriteDeadline(t.Add(writeTimeout)); err != nil {
					// do not panic here, since the error may
					// indicate that the connection is already closed
					return fmt.Errorf("cannot update write deadline: %s", err)
				}
				lastWriteDeadline = t
			}
		}

		if so != nil {
			header.SetControl(streamMessage)
			header.SetID(so.streamID)

			if _, err := bw.Write(header); err != nil {
				so.error <- err
				so = nil
				return fmt.Errorf("cannot write resp header: %s", err)
			}

			if err := so.response.WriteResponse(bw); err != nil {
				so.error <- err
				so = nil
				return fmt.Errorf("cannot write stream response: %s", err)
			}
			so.error <- nil
			ReleaseResponse(so.response)
			releaseServerStreamOut(so)

			flushCh = flushAlwaysCh
			so = nil
		} else {

			header.SetControl(unary)
			header.SetID(binary.BigEndian.Uint32(wi.reqID[:]))

			if _, err := bw.Write(header); err != nil {
				return fmt.Errorf("cannot write header: %s", err)
			}

			if err := wi.ctx.WriteResponse(bw); err != nil {
				return fmt.Errorf("cannot write response: %s", err)
			}

			s.releaseWorkItem(wi)

			// re-arm flush channel
			if flushCh == nil && len(pendingResponses) == 0 {
				if maxBatchDelay > 0 {
					resetFlushTimer(flushTimer, maxBatchDelay)
					flushCh = flushTimer.C
				} else {
					flushCh = flushAlwaysCh
				}
			}
		}

	}
}

type serverWorkItem struct {
	ctx         *exposedCtx
	startStream bool
	reqID       [4]byte
	streamID    [4]byte
}

func (s *Server) acquireWorkItem() *serverWorkItem {
	v := s.workItemPool.Get()
	if v == nil {
		return &serverWorkItem{
			ctx: s.NewHandlerCtx(),
		}
	}
	return v.(*serverWorkItem)
}

func (s *Server) releaseWorkItem(wi *serverWorkItem) {
	s.workItemPool.Put(wi)
}

var defaultLogger = zerolog.New(os.Stdout).With().Caller().Logger()

func (s *Server) logger() *zerolog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return &defaultLogger
}

func isZeroReqID(reqID [4]byte) bool {
	return reqID[0] == 0 && reqID[1] == 0 && reqID[2] == 0 && reqID[3] == 0
}

// MaxBatchDelay is the maximum duration before ready responses
// are sent to the client.
//
// Responses' batching may reduce network bandwidth usage and CPU usage.
//
// By default responses are sent immediately to the client.
func ServerMaxBatchDelay(d time.Duration) ServerOption {
	return func(options *serverOptions) {
		options.MaxBatchDelay = d
	}
}

//ServerCodec specifies the encoding codec to be used to marshal/unmarshal messages
//its possible to achieve zero copy with carefully written codecs
func ServerCodec(co string) ServerOption {
	return func(c *serverOptions) {
		c.Codec = encoding.GetCodec(co)
	}
}

//ServerCompression sets the compression type used by the transport
func ServerCompression(sc CompressType) ServerOption {
	return func(c *serverOptions) {
		c.CompressType = sc
	}
}

//ServerCompression sets the compression type used by the transport
func ServerTlsConfig(tlsc *tls.Config) ServerOption {
	return func(c *serverOptions) {
		c.TLSConfig = tlsc
	}
}

//ServerCompression sets the maximum number of concurrent requests before server return error
func ServerMaxConcurrency(maxConcurrency uint32) ServerOption {
	return func(c *serverOptions) {
		c.Concurrency = maxConcurrency
	}
}
