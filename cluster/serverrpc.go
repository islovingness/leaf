package cluster

import (
	"fmt"
	"github.com/name5566/leaf/chanrpc"
)

var (
	routeMap = map[interface{}]*chanrpc.Client{}
)

type RequestInfo struct{
	cb      interface{}
	chanRet chan *chanrpc.RetInfo
}

func GetRequestCount() int32 {
	agentsMutex.Lock()
	defer agentsMutex.Unlock()

	var count int32 = 0
	for _, agent := range agents {
		count += agent.getRequestCount()
	}
	return count
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
