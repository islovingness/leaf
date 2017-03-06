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
	routeMap		= map[interface{}]*chanrpc.Client{}
)

type RequestInfo struct{
	cb      interface{}
	chanRet chan *chanrpc.RetInfo
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

func GetRequestCount() int {
	requestMutex.Lock()
	defer requestMutex.Unlock()
	return len(requestMap)
}

func SetRoute(id interface{}, server *chanrpc.Server) {
	_, ok := routeMap[id]
	if ok {
		panic(fmt.Sprintf("function id %v: already set route", id))
	}

	routeMap[id] = server.Open(0)
}

func Go(serverName string, id interface{}, args ...interface{}) {
	agent := GetAgent(serverName)
	if agent != nil {
		agent.Go(id, args...)
	}
}

func Call0(serverName string, id interface{}, args ...interface{}) error {
	agent := GetAgent(serverName)
	if agent != nil {
		return agent.Call0(id, args...)
	} else {
		return fmt.Errorf("%v server is offline", serverName)
	}
}

func Call1(serverName string, id interface{}, args ...interface{}) (interface{}, error) {
	agent := GetAgent(serverName)
	if agent != nil {
		return agent.Call1(id, args...)
	} else {
		return nil, fmt.Errorf("%v server is offline", serverName)
	}
}

func CallN(serverName string, id interface{}, args ...interface{}) ([]interface{}, error) {
	agent := GetAgent(serverName)
	if agent != nil {
		return agent.CallN(id, args...)
	} else {
		return nil, fmt.Errorf("%v server is offline", serverName)
	}
}

func AsynCall(serverName string, chanAsynRet chan *chanrpc.RetInfo, id interface{}, args ...interface{}) {
	agent := GetAgent(serverName)
	if agent != nil {
		agent.AsynCall(chanAsynRet, id, args...)
	} else {
		chanAsynRet <- &chanrpc.RetInfo{
			Err:fmt.Errorf("%v server is offline", serverName),
			Cb:args[len(args) - 1],
		}
	}
}
