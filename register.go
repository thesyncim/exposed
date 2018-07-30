package exposed

import (
	"errors"

	"github.com/cespare/xxhash"
)

var (
	errNilOperationTypes     = errors.New("nil operation types")
	errNilOperationArgType   = errors.New("nil operation argument type")
	errNilOperationReplyType = errors.New("nil operation reply type")
)

//TypeMaker creates a new instance of the provided type
//is used to avoid reflection
type TypeMaker func() Message

var registeredOpTypes = map[uint64]*OperationTypes{}

type OperationTypes struct {
	ArgsType  TypeMaker
	ReplyType TypeMaker
}

func registerOperationInfo(name string, info *OperationTypes) error {
	if info == nil {
		return errNilOperationTypes
	}

	if info.ArgsType == nil {
		return errNilOperationArgType
	}

	if info.ReplyType == nil {
		return errNilOperationReplyType
	}

	registeredOpTypes[xxhash.Sum64String(name)] = info
	return nil
}

func getOperationInfo(name uint64) *OperationTypes {
	return registeredOpTypes[name]
}

type OperationInfo struct {
	Operation      string
	Handler        HandlerFunc
	OperationTypes *OperationTypes
}

var operationHandlers = map[uint64]HandlerFunc{}

func registerService(e Exposable) {
	ops := e.ExposedOperations()
	for i := range ops {
		registerHandleFunc(ops[i].Operation, ops[i].Handler, ops[i].OperationTypes)
	}
}

func registerHandleFunc(path string, handlerFunc HandlerFunc, info *OperationTypes) {
	op := xxhash.Sum64String(path)
	if _, ok := operationHandlers[op]; ok {
		panic(errOperationAlreadyRegistered.Error() + " " + path)
	}
	registerOperationInfo(path, info)
	operationHandlers[op] = handlerFunc
}

func match(operation uint64) (HandlerFunc, error) {
	h, ok := operationHandlers[(operation)]
	if !ok {
		return nil, errHandlerNotFound
	}
	return h, nil
}
