package exposed

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestServerBrokenClientCloseConn(t *testing.T) {
	testServerBrokenClient(t, func(conn net.Conn) error {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("cannot close server connection: %s", err)
		}
		return nil
	})
}

func TestServerBrokenClientGarbageRequest(t *testing.T) {
	testServerBrokenClient(t, func(conn net.Conn) error {
		_, err := conn.Write([]byte("garbage\nrequest"))
		if err != nil {
			return fmt.Errorf("cannot send garbage to the server: %s", err)
		}
		return nil
	})
}

func TestServerBrokenClientSendRequestAndCloseConn(t *testing.T) {
	testServerBrokenClient(t, func(conn net.Conn) error {
		var reqID [4]byte
		if _, err := conn.Write(reqID[:]); err != nil {
			return fmt.Errorf("cannot send reqID to the server: %s", err)
		}

		var req request
		req.Append([]byte("foobar"))
		bw := bufio.NewWriter(conn)
		if err := req.WriteRequest(bw); err != nil {
			return fmt.Errorf("cannot send request to the server: %s", err)
		}

		if err := conn.Close(); err != nil {
			return fmt.Errorf("cannot close server connection: %s", err)
		}
		return nil
	})
}

func newTestHandlerCtx() HandlerCtx {
	return &exposedCtx{
		Request:  acquireRequest(),
		Response: AcquireResponse(),
	}
}

func testServerBrokenClient(t *testing.T, clientConnFunc func(net.Conn) error) {
	l := zerolog.New(os.Stdout)

	s := NewServer(
		ServerCompression(CompressNone),
	)
	s.SniffHeader = "wqwer"
	s.ProtocolVersion = 123
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler
	s.Logger = &l
	serverStop, ln := newTestServerExt(s)

	clientStopCh := make(chan error, 1)
	go func() {
		conn, err := ln.Dial()
		if err != nil {
			clientStopCh <- err
			return
		}

		cfg := &handshakeConfig{
			sniffHeader:       []byte(s.SniffHeader),
			protocolVersion:   s.ProtocolVersion,
			conn:              conn,
			writeCompressType: s.opts.CompressType,
		}
		readCompressType, _, realConn, err := handshakeClient(cfg)
		if err != nil {
			clientStopCh <- err
			return
		}
		if readCompressType != s.opts.CompressType {
			clientStopCh <- fmt.Errorf("unexpected read CompressType: %v. Expecting %v", readCompressType, s.opts.CompressType)
			return
		}
		clientStopCh <- clientConnFunc(realConn)
	}()

	select {
	case err := <-clientStopCh:
		if err != nil {
			t.Fatalf("client error: %s", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerWithoutTLS(t *testing.T) {
	l := zerolog.New(ioutil.Discard)
	s := NewServer()
	s.Logger = &l
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler

	serverStop, c := newTestServerClientExt(s)
	ctlopt := ClientTLSConfig(&tls.Config{
		InsecureSkipVerify: true,
	})
	ctlopt(&c.opts)

	var req request
	var resp response

	for i := 0; i < 10; i++ {
		req.SwapValue([]byte("foobar"))
		err := c.DoDeadline(&req, &resp, time.Now().Add(time.Millisecond))
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerTLSUnencryptedConn(t *testing.T) {
	tlsConfig := newTestServerTLSConfig()

	s := NewServer(
		ServerTlsConfig(tlsConfig),
	)
	s.Handler = testEchoHandler
	s.NewHandlerCtx = newTestHandlerCtx

	serverStop, c := newTestServerClientExt(s)

	if err := testGet(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerTLSSerial(t *testing.T) {
	tlsConfig := newTestServerTLSConfig()
	s := NewServer(
		ServerTlsConfig(tlsConfig),
	)
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler

	serverStop, c := newTestServerClientExt(s)
	c.opts.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	if err := testGet(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerTLSConcurrent(t *testing.T) {
	tlsConfig := newTestServerTLSConfig()

	s := NewServer(
		ServerTlsConfig(tlsConfig),
	)
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler

	serverStop, c := newTestServerClientExt(s)
	c.opts.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	if err := testServerClientConcurrent(func() error { return testGet(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerNewCtxSerial(t *testing.T) {
	serverStop, c := newTestServerClient(testNewCtxHandler)

	if err := testNewCtx(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerNewCtxConcurrent(t *testing.T) {
	serverStop, c := newTestServerClient(testNewCtxHandler)

	if err := testServerClientConcurrent(func() error { return testNewCtx(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerTimeoutSerial(t *testing.T) {
	stopCh := make(chan struct{})
	h := func(ctx HandlerCtx) HandlerCtx {
		<-stopCh
		return ctx
	}
	serverStop, c := newTestServerClient(h)

	if err := testTimeout(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	close(stopCh)

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerTimeoutConcurrent(t *testing.T) {
	stopCh := make(chan struct{})
	h := func(ctx HandlerCtx) HandlerCtx {
		<-stopCh
		return ctx
	}
	serverStop, c := newTestServerClient(h)

	if err := testServerClientConcurrent(func() error { return testTimeout(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	close(stopCh)

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerBatchDelayRequestSerial(t *testing.T) {
	serverStop, c := newTestServerClient(testEchoHandler)
	c.opts.MaxBatchDelay = 10 * time.Millisecond

	if err := testGetBatchDelay(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerBatchDelayRequestConcurrent(t *testing.T) {
	serverStop, c := newTestServerClient(testEchoHandler)
	c.opts.MaxBatchDelay = 10 * time.Millisecond

	if err := testServerClientConcurrent(func() error { return testGetBatchDelay(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerBatchDelayResponseSerial(t *testing.T) {
	s := NewServer(
		ServerMaxBatchDelay(10 * time.Millisecond),
	)
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler

	serverStop, c := newTestServerClientExt(s)

	if err := testGetBatchDelay(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerBatchDelayResponseConcurrent(t *testing.T) {
	s := NewServer(ServerMaxBatchDelay(10 * time.Millisecond))
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler

	serverStop, c := newTestServerClientExt(s)

	if err := testServerClientConcurrent(func() error { return testGetBatchDelay(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerBatchDelayRequestResponseSerial(t *testing.T) {
	s := NewServer(ServerMaxBatchDelay(10 * time.Millisecond))
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = testEchoHandler
	serverStop, c := newTestServerClientExt(s)
	c.opts.MaxBatchDelay = 10 * time.Millisecond

	if err := testGetBatchDelay(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerBatchDelayRequestResponseConcurrent(t *testing.T) {
	s := NewServer(ServerMaxBatchDelay(10 * time.Millisecond))
	s.Handler = testEchoHandler
	s.NewHandlerCtx = newTestHandlerCtx

	serverStop, c := newTestServerClientExt(s)
	c.opts.MaxBatchDelay = 10 * time.Millisecond

	if err := testServerClientConcurrent(func() error { return testGetBatchDelay(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerCompressNoneSerial(t *testing.T) {
	testServerCompressSerial(t, CompressNone, CompressNone)
}

func TestServerCompressNoneConcurrent(t *testing.T) {
	testServerCompressConcurrent(t, CompressNone, CompressNone)
}

func TestServerCompressFlateSerial(t *testing.T) {
	testServerCompressSerial(t, CompressFlate, CompressFlate)
}

func TestServerCompressFlateConcurrent(t *testing.T) {
	testServerCompressConcurrent(t, CompressFlate, CompressFlate)
}

func TestServerCompressSnappySerial(t *testing.T) {
	testServerCompressSerial(t, CompressSnappy, CompressSnappy)
}

func TestServerCompressSnappyConcurrent(t *testing.T) {
	testServerCompressConcurrent(t, CompressSnappy, CompressSnappy)
}

func TestServerCompressMixedSerial(t *testing.T) {
	testServerCompressSerial(t, CompressSnappy, CompressFlate)
	testServerCompressSerial(t, CompressNone, CompressFlate)
	testServerCompressSerial(t, CompressFlate, CompressSnappy)
	testServerCompressSerial(t, CompressSnappy, CompressNone)
}

func TestServerCompressMixedConcurrent(t *testing.T) {
	testServerCompressConcurrent(t, CompressSnappy, CompressFlate)
	testServerCompressConcurrent(t, CompressNone, CompressFlate)
	testServerCompressConcurrent(t, CompressFlate, CompressSnappy)
	testServerCompressConcurrent(t, CompressSnappy, CompressNone)
}

func testServerCompressSerial(t *testing.T, reqCompressType, respCompressType CompressType) {
	s := NewServer(ServerCompression(respCompressType))
	s.Handler = testEchoHandler
	s.NewHandlerCtx = newTestHandlerCtx

	serverStop, c := newTestServerClientExt(s)
	c.opts.CompressType = reqCompressType

	if err := testGet(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func testServerCompressConcurrent(t *testing.T, reqCompressType, respCompressType CompressType) {
	s := NewServer(ServerCompression(respCompressType))
	s.Handler = testEchoHandler
	s.NewHandlerCtx = newTestHandlerCtx

	serverStop, c := newTestServerClientExt(s)
	c.opts.CompressType = reqCompressType

	if err := testServerClientConcurrent(func() error { return testGet(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerConcurrencyLimit(t *testing.T) {
	const concurrency = 10
	doneCh := make(chan struct{})
	concurrencyCh := make(chan struct{}, concurrency)

	s := NewServer()
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = func(ctxv HandlerCtx) HandlerCtx {
		concurrencyCh <- struct{}{}
		<-doneCh
		ctx := ctxv.(*exposedCtx)
		ctx.Response.Write([]byte("done"))
		return ctx
	}

	s.opts.Concurrency = concurrency

	serverStop, c := newTestServerClientExt(s)

	// issue concurrency requests to the server.
	resultCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			var req request
			var resp response
			req.SetOperation(4)
			if err := c.DoDeadline(&req, &resp, time.Now().Add(time.Hour)); err != nil {
				resultCh <- err
				return
			}
			if string(resp.Payload()) != "done" {
				resultCh <- fmt.Errorf("unexpected body: %q. Expecting %q", resp.Payload(), "done")
				return
			}
			resultCh <- nil
		}()
	}

	// make sure the server called request handler for the issued requests
	for i := 0; i < concurrency; i++ {
		select {
		case <-concurrencyCh:
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout on iteration %d", i)
		}
	}

	// now all the requests must fail with 'concurrency limit exceeded'
	// error.
	for i := 0; i < 100; i++ {
		var req request
		var resp response
		req.Append([]byte("aaa.bbb"))
		if err := c.DoDeadline(&req, &resp, time.Now().Add(time.Second)); err != nil {
			t.Fatalf("unexpected error on iteration %d: %s", i, err)
		}
		if string(resp.Error()) != "max concurrency excedded" {
			t.Fatalf("unexpected response on iteration %d: %q. Expecting %q", i, resp.Error(), "too many requests")
		}
	}

	// unblock requests to the server.
	close(doneCh)
	for i := 0; i < concurrency; i++ {
		select {
		case err := <-resultCh:
			if err != nil {
				t.Fatalf("unexpected error on iteration %d: %s", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout on iteration %d", i)
		}
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerClientSendNowait(t *testing.T) {
	const iterations = 100
	const concurrency = 10
	callsCh := make(chan struct{}, concurrency*iterations)
	h := func(ctxv HandlerCtx) HandlerCtx {
		ctx := ctxv.(*exposedCtx)
		s := string(ctx.Request.Payload())
		if strings.HasPrefix(s, "foobar ") {
			ctx.Response.Write([]byte(s))
		}
		callsCh <- struct{}{}
		return ctx
	}
	serverStop, c := newTestServerClient(h)

	err := testServerClientConcurrentExt(func() error {
		var resp response
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				req := acquireTestRequest()
				req.SwapValue([]byte("nowait!!!"))
				if !c.SendNowait(req, releaseTestRequest) {
					return fmt.Errorf("cannot enqueue new request to SendNowait")
				}
			} else {
				var req request
				s := fmt.Sprintf("foobar %d", i)
				req.SwapValue([]byte(s))
				err := c.DoDeadline(&req, &resp, time.Now().Add(time.Second))
				if err != nil {
					return fmt.Errorf("unexpected error in DoDeadline on iteration %d: %s", i, err)
				}
				if string(resp.Payload()) != s {
					return fmt.Errorf("unexpected body on iteration %d: %q. Expecting %q", i, resp.Payload(), s)
				}
			}
		}
		return nil
	}, concurrency)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for i := 0; i < concurrency*iterations; i++ {
		select {
		case <-callsCh:
		case <-time.After(time.Second):
			t.Fatalf("timeout on iteration %d", i)
		}
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerEchoSerial(t *testing.T) {
	serverStop, c := newTestServerClient(testEchoHandler)

	if err := testGet(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerEchoConcurrent(t *testing.T) {
	serverStop, c := newTestServerClient(testEchoHandler)

	if err := testServerClientConcurrent(func() error { return testGet(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerSleepSerial(t *testing.T) {
	serverStop, c := newTestServerClient(testSleepHandler)

	if err := testSleep(c); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerSleepConcurrent(t *testing.T) {
	serverStop, c := newTestServerClient(testSleepHandler)

	if err := testServerClientConcurrent(func() error { return testSleep(c) }); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerMultiClientsSerial(t *testing.T) {
	serverStop, ln := newTestServer(testSleepHandler)

	f := func() error {
		c := newTestClient(ln)
		return testSleep(c)
	}
	if err := testServerClientConcurrent(f); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func TestServerMultiClientsConcurrent(t *testing.T) {
	serverStop, ln := newTestServer(testSleepHandler)

	f := func() error {
		c := newTestClient(ln)
		return testServerClientConcurrent(func() error { return testSleep(c) })
	}
	if err := testServerClientConcurrent(f); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

func testServerClientConcurrent(testFunc func() error) error {
	return testServerClientConcurrentExt(testFunc, 10)
}

func testServerClientConcurrentExt(testFunc func() error, concurrency int) error {
	resultCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			resultCh <- testFunc()
		}()
	}

	for i := 0; i < concurrency; i++ {
		select {
		case err := <-resultCh:
			if err != nil {
				return fmt.Errorf("unexpected error: %s", err)
			}
		case <-time.After(time.Second):
			return fmt.Errorf("timeout")
		}
	}
	return nil
}

func testGet(c *Client) error {
	return testGetExt(c, 100)
}

func testGetBatchDelay(c *Client) error {
	return testGetExt(c, 10)
}

func testGetExt(c *Client, iterations int) error {
	var req request
	var resp response
	for i := 0; i < iterations; i++ {
		s := fmt.Sprintf("foobar %d", i)
		req.SwapValue([]byte(s))
		err := c.DoDeadline(&req, &resp, time.Now().Add(time.Second))
		if err != nil {
			return fmt.Errorf("unexpected error on iteration %d: %s", i, err)
		}
		if string(resp.Payload()) != s {
			return fmt.Errorf("unexpected body on iteration %d: %q. Expecting %q", i, resp.Payload(), s)
		}
	}
	return nil
}

func testSleep(c *Client) error {
	var (
		req  request
		resp response
	)
	expectedBodyPrefix := []byte("slept for ")
	for i := 0; i < 10; i++ {
		req.SwapValue([]byte("fobar"))
		err := c.DoDeadline(&req, &resp, time.Now().Add(time.Second))
		if err != nil {
			return fmt.Errorf("unexpected error on iteration %d: %s", i, err)
		}
		if !bytes.HasPrefix(resp.Payload(), expectedBodyPrefix) {
			return fmt.Errorf("unexpected body prefix on iteration %d: %q. Expecting %q", i, resp.Payload(), expectedBodyPrefix)
		}
	}
	return nil
}

func testTimeout(c *Client) error {
	var (
		req  request
		resp response
	)
	for i := 0; i < 10; i++ {
		req.SwapValue([]byte("fobar"))
		err := c.DoDeadline(&req, &resp, time.Now().Add(10*time.Millisecond))
		if err == nil {
			return fmt.Errorf("expecting non-nil error on iteration %d", i)
		}
		if err != ErrTimeout {
			return fmt.Errorf("unexpected error: %s. Expecting %s", err, ErrTimeout)
		}
	}
	return nil
}

func testNewCtx(c *Client) error {
	var (
		req  request
		resp response
	)
	for i := 0; i < 10; i++ {
		req.SwapValue([]byte("fobar"))
		err := c.DoDeadline(&req, &resp, time.Now().Add(100*time.Millisecond))
		if err != nil {
			return fmt.Errorf("unexpected error on iteration %d: %s", i, err)
		}
		if string(resp.Payload()) != "new ctx!" {
			return fmt.Errorf("unexpected body on iteration %d: %q. Expecting %q", i, resp.Payload(), "new ctx!")
		}
	}
	return nil
}

func newTestServerClient(handler func(HandlerCtx) HandlerCtx) (func() error, *Client) {
	serverStop, ln := newTestServer(handler)
	c := newTestClient(ln)
	return serverStop, c
}

func newTestServerClientExt(s *Server) (func() error, *Client) {
	serverStop, ln := newTestServerExt(s)
	c := newTestClient(ln)
	return serverStop, c
}

func newTestServer(handler func(HandlerCtx) HandlerCtx) (func() error, *fasthttputil.InmemoryListener) {
	s := NewServer()
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = handler

	return newTestServerExt(s)
}

func newTestServerExt(s *Server) (func() error, *fasthttputil.InmemoryListener) {
	ln := fasthttputil.NewInmemoryListener()
	serverResultCh := make(chan error, 1)
	go func() {
		serverResultCh <- s.Serve(ln)
	}()

	return func() error {
		ln.Close()
		select {
		case err := <-serverResultCh:
			if err != nil {
				return fmt.Errorf("unexpected error: %s", err)
			}
		case <-time.After(time.Second):
			return fmt.Errorf("timeout")
		}
		return nil
	}, ln
}

func newTestClient(ln *fasthttputil.InmemoryListener) *Client {

	c := NewClient("", ClientDialer(func(addr string) (net.Conn, error) {
		return ln.Dial()
	}))
	c.NewResponse = newTestResponse
	return c
}

func testNewCtxHandler(ctxv HandlerCtx) HandlerCtx {
	ctxvNew := newTestHandlerCtx()
	ctx := ctxvNew.(*exposedCtx)
	ctx.Response.Write([]byte("new ctx!"))
	return ctx
}

func testEchoHandler(ctxv HandlerCtx) HandlerCtx {
	ctx := ctxv.(*exposedCtx)
	ctx.Response.Write(ctx.Request.Payload())
	return ctx
}

func testSleepHandler(ctxv HandlerCtx) HandlerCtx {
	sleepDuration := time.Duration(rand.Intn(30)) * time.Millisecond
	time.Sleep(sleepDuration)
	s := fmt.Sprintf("slept for %s", sleepDuration)
	ctx := ctxv.(*exposedCtx)
	ctx.Response.Write([]byte(s))
	return ctx
}

func newTestServerTLSConfig() *tls.Config {
	tlsCertFile := "./ssl-cert-exposed.pem"
	tlsKeyFile := "./ssl-cert-exposed.key"
	cert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
	if err != nil {
		panic(fmt.Sprintf("cannot load TLS key pair from certFile=%q and keyFile=%q: %s", tlsCertFile, tlsKeyFile, err))
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	return tlsConfig
}

func acquireTestRequest() *request {
	return acquireRequest()
}

func releaseTestRequest(req requestWriter) {
	releaseRequest(req.(*request))
}
