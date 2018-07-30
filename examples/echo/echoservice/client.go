package echoservice

import "github.com/thesyncim/exposed"

type EchoserviceClient struct {
	*exposed.Client
}

func NewClient(c *exposed.Client) *EchoserviceClient {
	return &EchoserviceClient{c}
}
func (c *EchoserviceClient) Echo(msg []byte) (ret []byte, err error) {
	var req = &EchoRequest{Msg: msg}
	var resp = &EchoReply{}
	err = c.Call("echoservice.Echo", req, resp)
	ret = resp.Ret
	return
}
