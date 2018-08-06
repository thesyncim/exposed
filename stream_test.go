package exposed

import (
	"github.com/pkg/profile"
	"github.com/thesyncim/exposed/encoding/codec/json"
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
		ServerCodec(json.CodecName),
	)

	c = NewClient("127.0.0.1:7654",
		ClientCodec(json.CodecName),
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
	p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	defer p.Stop()

	s.HandleFunc("stream", func(ctx *Context, req Message, resp Message) (err error) {
		var m string
		if err := ctx.Stream.RecvMsg(&m); err != nil {
			return err
		}

		n, err := strconv.Atoi(*req.(*string))
		log.Println(n)

		for i := 0; i < n; i++ {
			err = ctx.Stream.SendMsg(&m)
			if err != nil {
				t.Fatal(err)
			}
			if i == 99 {

			}
		}
		(*resp.(*string)) = m

		return err
	}, &OperationTypes{
		ReplyType: func() Message {
			return new(string)
		},
		ArgsType: func() Message {
			return new(string)
		},
	})

	var request = strconv.Itoa(t.N / 2)
	var response = ""

	err := c.CallStream("stream", &request, &response, func(client *StreamClient) error {
		t.ResetTimer()
		t.ReportAllocs()

		err := client.SendMsg(request)
		if err != nil {
			return err
		}

		var r string

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
	log.Println(response)
}
