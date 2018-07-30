package echoservice

import (
	"github.com/thesyncim/exposed"
	"github.com/thesyncim/exposed/examples/echo"
)

type EchoserviceServer struct {
	Impl echo.Echoer
}

func NewServer(Impl echo.Echoer) *EchoserviceServer {
	return &EchoserviceServer{Impl}
}
func (r *EchoserviceServer) ExposedOperations() []exposed.OperationInfo {
	var ops = make([]exposed.OperationInfo, 0)
	ops = append(ops, exposed.OperationInfo{
		Handler:   r.Echo,
		Operation: "echoservice.Echo",
		OperationTypes: &exposed.OperationTypes{
			ArgsType: func() exposed.Message {
				return new(EchoRequest)
			},
			ReplyType: func() exposed.Message {
				return new(EchoReply)
			},
		},
	})
	return ops
}
func (r *EchoserviceServer) Echo(ctx *exposed.Context, req exposed.Message, resp exposed.Message) (err error) {
	var _resp = resp.(*EchoReply)
	var msg = req.(*EchoRequest).Msg
	_resp.Ret = r.Impl.Echo(msg)
	return
}
