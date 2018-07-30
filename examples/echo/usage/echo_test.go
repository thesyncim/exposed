package usage

import (
	"crypto/rand"
	"log"
	"runtime"
	"testing"

	"github.com/pkg/profile"
	"github.com/thesyncim/exposed"
	"github.com/thesyncim/exposed/examples/echo/echoservice"
	"github.com/thesyncim/exposed/examples/echo/ecodec"
)

var s *exposed.Server
var c *exposed.Client
var cp *exposed.Client

func init() {
	/*log.SetFlags(log.Lshortfile)
	ln, err := net.Listen("tcp", "127.0.0.1:5555")
	if err != nil {
		panic(err)
	}
	s = exposed.NewServer(
		exposed.ServerCodec(ecodec.CodecName),
		exposed.ServerCompression(exposed.CompressNone),
	)
	if err != nil {
		panic(err)
	}

	simpleService := echoservice.NewServer(echo.Echo{})
	s.RegisterService(simpleService)

	go func() {
		log.Print(s.Serve(ln))
	}()*/

	c = exposed.NewClient("127.0.0.1:5555", exposed.ClientCodec(ecodec.CodecName), exposed.ClientCompression(exposed.CompressNone))
	cp = exposed.NewClient("127.0.0.1:5555", exposed.ClientCodec(ecodec.CodecName), exposed.PipelineRequests(true), exposed.ClientCompression(exposed.CompressNone))

}

func BenchmarkSimple(b *testing.B) {
	runtime.GOMAXPROCS(1)

	client := echoservice.NewClient(c)

	var payload = []byte("echo")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := client.Echo(payload)
		if err != nil {
			b.Fatal(err)
		}

	}
}

func BenchmarkSimpleParallel(b *testing.B) {

	p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	defer p.Stop()
	runtime.GOMAXPROCS(8)
	log.SetFlags(log.Lshortfile)

	client := echoservice.NewClient(c)

	b.Run("8bytePayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(8, 24, b, client)

	})
	b.Run("128bytePayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(128, 24, b, client)

	})
	b.Run("1KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(1024, 24, b, client)

	})

	b.Run("4KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(4096, 24, b, client)

	})
	b.Run("32KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(32*1024, 24, b, client)

	})
	b.Run("64KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(64*1024, 24, b, client)

	})

	b.Run("128KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(128*1024, 1, b, client)
	})
}

func BenchmarkSimpleParallelPipeline(b *testing.B) {

	p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	defer p.Stop()
	runtime.GOMAXPROCS(8)
	log.SetFlags(log.Lshortfile)

	client := echoservice.NewClient(cp)

	b.Run("8bytePayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(8, 24, b, client)

	})
	b.Run("128bytePayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(128, 24, b, client)
	})
	b.Run("1KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(1024, 24, b, client)
	})

	b.Run("4KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(4096, 24, b, client)

	})
	b.Run("32KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(32*1024, 24, b, client)

	})
	b.Run("64KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(64*1024, 24, b, client)

	})

	b.Run("128KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(128*1024, 0, b, client)
	})

	b.Run("128KBPayloadClientConcurrency24", func(b *testing.B) {
		benchmarkEcho(1024*1024, 0, b, client)
	})
}
func benchmarkEcho(psize int64, concurrency int64, b *testing.B, c *echoservice.EchoserviceClient) {

	var payload = make([]byte, psize)
	rand.Read(payload)
	b.ReportAllocs()

	b.SetParallelism(int(concurrency))

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))
	b.RunParallel(func(pb *testing.PB) {

		//var result []byte
		for pb.Next() {
			_, err := c.Echo(payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
