package proto

import (
	"github.com/gogo/protobuf/proto"
	"github.com/thesyncim/exposed/encoding"
)

const CodecName = "protobuf"

func init() {
	encoding.RegisterCodec(ProtoCodec{})
}

type ProtoCodec struct{}

func (ProtoCodec) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

func (ProtoCodec) Unmarshal(data []byte, v interface{}) error {
	protoMsg := v.(proto.Message)

	if pu, ok := protoMsg.(proto.Unmarshaler); ok {
		// object can unmarshal itself, no need for buffer
		return pu.Unmarshal(data)
	}

	panic("compile with gogoproto")
}
func (ProtoCodec) Name() string {
	return CodecName
}
