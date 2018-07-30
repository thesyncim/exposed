package echoservice

// protobuf=true
type EchoRequest struct {
	Msg []byte `protobuf:"bytes,1,opt,name=msg"`
}

// protobuf=true
type EchoReply struct {
	Ret []byte `protobuf:"bytes,1,opt,name=ret"`
}
