Exposed - minimal high performant reflectionless RPC Server

[![Build Status](https://travis-ci.org/thesyncim/exposed.svg?branch=master)](https://travis-ci.org/thesyncim/exposed) 
## Getting Started

### Installing 
To start using exposed, install Go 1.10 or above and run:

```sh
go get github.com/thesyncim/exposed
```

This will retrieve the library and install it.

### Usage exposed is an high level RPC framework.

example server:

```go
package main

import (
        "log"
        "net"

        "github.com/thesyncim/exposed"
        "github.com/thesyncim/exposed/codec/json"
)

func main() {
        ln, err := net.Listen("tcp", "127.0.0.1:8888")
        if err != nil {
                panic(err)
        }

        server := exposed.NewServer(
                exposed.ServerCodec(json.CodecName),
                //exposed.ServerCompression(exposed.CompressSnappy),
        )

        server.RegisterHandleFunc("echo",
                func(ctx *exposed.Context, req exposed.Message, resp exposed.Message) (err error) {
                        *resp.(*string) = *req.(*string)
                        return nil
                },
                &exposed.OperationTypes{
                        ReplyType: func() exposed.Message {
                                return new(string)
                        },
                        ArgsType: func() exposed.Message {
                                return new(string)
                        },
                })

        log.Fatalln(server.Serve(ln))
}
```

example client:
```go
package main

import (
        "fmt"

        "github.com/thesyncim/exposed"
        "github.com/thesyncim/exposed/codec/json"
)

func main() {
        client := exposed.NewClient("127.0.0.1:8888",
                exposed.ClientCodec(json.CodecName),
                //exposed.ServerCompression(exposed.CompressSnappy),
        )

        var req = "ping"
        var resp string

        err := client.Call("echo", &req, &resp)
        if err != nil {
                panic(err)
        }

        fmt.Println(resp)
}
```

