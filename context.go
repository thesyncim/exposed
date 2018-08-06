package exposed

//context is passed to every Handler
type Context struct {
	//StreamServer    StreamServer
	Stream Stream
}

type Stream interface {
	SendMsg(m Message) error
	RecvMsg(m Message) (err error)
}
