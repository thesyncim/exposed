package exposed

import (
	"github.com/thesyncim/exposed/encoding/codec/proto"
	"github.com/thesyncim/exposed/test/echoservice"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
)

var s *Server
var c *Client
var ln net.Listener

func init() {
	log.SetFlags(log.Lshortfile)
	var err error
	ln, err = net.Listen("tcp", "127.0.0.1:7654")
	if err != nil {
		log.Fatal(err)
	}

	s = NewServer(
		ServerCodec(proto.CodecName),
	)

	c = NewClient("127.0.0.1:7654",
		ClientCodec(proto.CodecName),
	)
}

var sOnce sync.Once

func serveOnce() {
	sOnce.Do(func() {
		go func() {
			log.Fatal(s.Serve(ln))
		}()
	})
}

func BenchmarkStream(t *testing.B) {
	serveOnce()
	/*p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	defer p.Stop()*/

	s.HandleFunc("stream", func(ctx *Context, req Message, resp Message) (err error) {
		var m echoservice.EchoRequest
		if err := ctx.Stream.RecvMsg(&m); err != nil {
			return err
		}

		n, err := strconv.Atoi(string(req.(*echoservice.EchoRequest).Msg))

		out := echoservice.EchoReply{}
		for i := 0; i < n; i++ {
			err = ctx.Stream.SendMsg(&out)
			if err != nil {
				t.Fatal(err)
			}
			if i == 99 {

			}
		}
		(*resp.(*echoservice.EchoReply)) = out

		return err
	}, &OperationTypes{
		ReplyType: func() Message {
			return new(echoservice.EchoReply)
		},
		ArgsType: func() Message {
			return new(echoservice.EchoRequest)
		},
	})

	var request = echoservice.EchoRequest{Msg: []byte(strconv.Itoa(t.N / 2))}
	var response echoservice.EchoReply
	var r echoservice.EchoReply
	err := c.CallStream("stream", &request, &response, func(client *StreamClient) error {
		t.ResetTimer()
		t.ReportAllocs()

		err := client.SendMsg(&request)
		if err != nil {
			return err
		}

		for i := 0; i < t.N/2; i++ {
			err = client.RecvMsg(&r)

			if err != nil {
				t.Fatal(err, i)
			}
		}
		t.StopTimer()

		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
