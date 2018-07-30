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
	return proto.Unmarshal(data, v.(proto.Message))
}
func (ProtoCodec) Name() string {
	return CodecName
}
