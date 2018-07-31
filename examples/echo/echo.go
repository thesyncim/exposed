//go:generate go install github.com/thesyncim/exposed/expose
//go:generate expose gen -i  Echoer -p github.com/thesyncim/exposed/examples/echo -s echoservice -o echoservice

package echo

type Echoer interface {
	Echo(msg []byte) (ret []byte)
}

type Echo struct {
}

func (Echo) Echo(msg []byte) []byte {
	return msg
}
