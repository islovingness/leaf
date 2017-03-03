package cluster

import (
	"fmt"
	"github.com/name5566/leaf/chanrpc"
	"sync"
)

var (
	requestID 		uint32
	requestMutex	sync.Mutex
	requestMap 		= map[uint32]*RequestInfo{}
	routeMap		= map[interface{}]*chanrpc.Server{}
)

type RequestInfo struct{
	cb      interface{}
	chanRet chan *chanrpc.RetInfo
}

func SetRoute(id interface{}, server *chanrpc.Server) {
	_, ok := routeMap[id]
	if ok {
		panic(fmt.Sprintf("function id %v: already set route", id))
	}

	routeMap[id] = server
}

func registerRequest(request *RequestInfo) uint32 {
	requestMutex.Lock()
	defer requestMutex.Unlock()

	reqID := requestID
	requestMap[reqID] = request
	requestID += 1
	return reqID
}

func popRequest(requestID uint32) *RequestInfo {
	requestMutex.Lock()
	defer requestMutex.Unlock()

	request, ok := requestMap[requestID]
	if ok {
		delete(requestMap, requestID)
		return request
	} else {
		return nil
	}
}