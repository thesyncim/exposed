package ecodec

import (
	"errors"
	"github.com/thesyncim/exposed/encoding"
	"github.com/thesyncim/exposed/examples/echo/echoservice"
)

func init() {
	encoding.RegisterCodec(echocodec{})

}

const CodecName = "echoservice"

type echocodec struct{}

func (echocodec) Marshal(v interface{}) ([]byte, error) {
	switch v := v.(type) {
	case *echoservice.EchoRequest:
		return v.Msg, nil
	case *echoservice.EchoReply:
		return v.Ret, nil
	}
	return nil, errors.New("invalid type")
}

func (echocodec) Unmarshal(data []byte, v interface{}) error {
	switch v := v.(type) {
	case *echoservice.EchoRequest:
		v.Msg = data
		return nil
	case *echoservice.EchoReply:
		v.Ret = data
		return nil
	}
	return errors.New("invalid type")
}

func (echocodec) Name() string {
	return CodecName
}
