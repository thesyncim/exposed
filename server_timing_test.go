package exposed

import (
	"bytes"
	"crypto/tls"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"math/rand"
)

func BenchmarkEndToEndNoDelay1(b *testing.B) {
	benchmarkEndToEnd(b, 1, 0, CompressNone, false, false)
}

func BenchmarkEndToEndNoDelay10(b *testing.B) {
	benchmarkEndToEnd(b, 10, 0, CompressNone, false, false)
}

func BenchmarkEndToEndNoDelay100(b *testing.B) {
	benchmarkEndToEnd(b, 100, 0, CompressNone, false, false)
}

func BenchmarkEndToEndNoDelay1000(b *testing.B) {
	benchmarkEndToEnd(b, 1000, 0, CompressNone, false, false)
}

func BenchmarkEndToEndNoDelay10K(b *testing.B) {
	benchmarkEndToEnd(b, 10000, 0, CompressNone, false, false)
}

func BenchmarkEndToEndDelay1ms(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressNone, false, false)
}

func BenchmarkEndToEndDelay2ms(b *testing.B) {
	benchmarkEndToEnd(b, 1000, 2*time.Millisecond, CompressNone, false, false)
}

func BenchmarkEndToEndDelay4ms(b *testing.B) {
	benchmarkEndToEnd(b, 1000, 4*time.Millisecond, CompressNone, false, false)
}

func BenchmarkEndToEndDelay8ms(b *testing.B) {
	benchmarkEndToEnd(b, 1000, 8*time.Millisecond, CompressNone, false, false)
}

func BenchmarkEndToEndDelay16ms(b *testing.B) {
	benchmarkEndToEnd(b, 1000, 16*time.Millisecond, CompressNone, false, false)
}

func BenchmarkEndToEndCompressNone(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressNone, false, false)
}

func BenchmarkEndToEndCompressFlate(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressFlate, false, false)
}

func BenchmarkEndToEndCompressSnappy(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressSnappy, false, false)
}

func BenchmarkEndToEndTLSCompressNone(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressNone, true, false)
}

func BenchmarkEndToEndTLSCompressFlate(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressFlate, true, false)
}

func BenchmarkEndToEndTLSCompressSnappy(b *testing.B) {
	benchmarkEndToEnd(b, 1000, time.Millisecond, CompressSnappy, true, false)
}

func BenchmarkEndToEndPipeline1(b *testing.B) {
	benchmarkEndToEnd(b, 1, 0, CompressNone, false, true)
}

func BenchmarkEndToEndPipeline10(b *testing.B) {
	benchmarkEndToEnd(b, 10, 0, CompressNone, false, true)
}

func BenchmarkEndToEndPipeline100(b *testing.B) {
	benchmarkEndToEnd(b, 100, 0, CompressNone, false, true)
}

func BenchmarkEndToEndPipeline1000(b *testing.B) {
	benchmarkEndToEnd(b, 1000, 0, CompressNone, false, true)
}

func BenchmarkSendNowait(b *testing.B) {
	bN := uint64(b.N)
	var n uint64
	doneCh := make(chan struct{})

	s := NewServer(
		ServerMaxConcurrency(uint32(runtime.GOMAXPROCS(-1) + 1)),
	)
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = func(ctxv *exposedCtx) *exposedCtx {
		x := atomic.AddUint64(&n, 1)
		if x == bN {
			close(doneCh)
		}
		return ctxv
	}

	serverStop, ln := newTestServerExt(s)

	value := []byte("foobar")
	b.RunParallel(func(pb *testing.PB) {
		c := newTestClient(ln)
		c.opts.MaxPendingRequests = 1e2
		c.opts.PipelineRequests = true
		c.opts.CompressType = CompressNone
		for pb.Next() {
			for {
				req := acquireTestRequest()
				req.Append(value)
				if c.SendNowait(req, releaseTestRequest) {
					break
				}
				runtime.Gosched()
			}
		}
	})
	runtime.Gosched()

	// Add skipped requests.
	// Requests may be skipped by cleaners.
	c := newTestClient(ln)
	for {
		x := atomic.LoadUint64(&n)
		if x >= bN {
			break
		}
		req := acquireTestRequest()
		req.Append(value)
		c.SendNowait(req, releaseTestRequest)
		runtime.Gosched()
	}

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		b.Fatalf("timeout. n=%d, b.N=%d", n, b.N)
	}

	if err := serverStop(); err != nil {
		b.Fatalf("cannot shutdown server: %s", err)
	}
}

func benchmarkEndToEnd(b *testing.B, parallelism int, batchDelay time.Duration, compressType CompressType, isTLS, pipelineRequests bool) {
	var tlsConfig *tls.Config
	if isTLS {
		tlsConfig = newTestServerTLSConfig()
	}
	var serverBatchDelay time.Duration
	if batchDelay > 0 {
		serverBatchDelay = batchDelay
	}

	var expectedBody = make([]byte, 128)
	rand.Read(expectedBody)
	s := NewServer(
		ServerMaxConcurrency(uint32(parallelism*runtime.NumCPU())),
		ServerMaxBatchDelay(serverBatchDelay),
		ServerCompression(compressType),
		ServerTlsConfig(tlsConfig),
	)
	s.NewHandlerCtx = newTestHandlerCtx
	s.Handler = func(ctxv *exposedCtx) *exposedCtx {
		ctx := ctxv
		ctx.Response.SwapPayload(expectedBody)
		return ctx
	}

	serverStop, ln := newTestServerExt(s)

	var cc []*Client
	for i := 0; i < runtime.NumCPU(); i++ {
		c := newTestClient(ln)
		c.opts.MaxPendingRequests = int(s.opts.Concurrency)
		c.opts.MaxBatchDelay = batchDelay
		c.opts.PipelineRequests = pipelineRequests

		c.opts.CompressType = compressType
		if isTLS {
			c.opts.TLSConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		cc = append(cc, c)
	}
	var clientIdx uint32

	deadline := time.Now().Add(time.Hour)
	b.SetParallelism(parallelism)
	b.SetBytes(int64(len(expectedBody)))
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		n := atomic.AddUint32(&clientIdx, 1)
		c := cc[int(n)%len(cc)]
		var req request
		var resp response
		req.SwapPayload([]byte("foobar"))
		for pb.Next() {
			if err := c.DoDeadline(&req, &resp, deadline); err != nil {
				b.Fatalf("unexpected error: %s", err)
			}
			if !bytes.Equal(resp.Payload(), expectedBody) {
				b.Fatalf("unexpected body: %q. Expecting %q", resp.Payload(), expectedBody)
			}
		}
	})

	if err := serverStop(); err != nil {
		b.Fatalf("cannot shutdown server: %s", err)
	}
}
