package json

import (
	"encoding/json"

	"github.com/thesyncim/exposed/encoding"
)

const CodecName = "json"

func init() {
	encoding.RegisterCodec(JsonCodec{})
}

type JsonCodec struct{}

func (JsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (JsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (JsonCodec) Name() string {
	return CodecName
}
