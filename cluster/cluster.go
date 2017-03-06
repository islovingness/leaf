package cluster

import (
	"math"
	"time"
	"reflect"
	"net"
	"fmt"
	"github.com/name5566/leaf/log"
	"github.com/name5566/leaf/conf"
	"github.com/name5566/leaf/network"
	"github.com/name5566/leaf/chanrpc"
)

var (
	server  *network.TCPServer
	clients []*network.TCPClient
	agents  = map[string]*Agent{}
)

func Init() {
	if conf.ListenAddr != "" {
		server = new(network.TCPServer)
		server.Addr = conf.ListenAddr
		server.MaxConnNum = int(math.MaxInt32)
		server.PendingWriteNum = conf.PendingWriteNum
		server.LenMsgLen = 4
		server.MaxMsgLen = math.MaxUint32
		server.NewAgent = newAgent

		server.Start()
	}

	for _, addr := range conf.ConnAddrs {
		client := new(network.TCPClient)
		client.Addr = addr
		client.ConnectInterval = 3 * time.Second
		client.PendingWriteNum = conf.PendingWriteNum
		client.LenMsgLen = 4
		client.MaxMsgLen = math.MaxUint32
		client.NewAgent = newAgent

		client.Start()
		clients = append(clients, client)
	}
}

func GetAgent(serverName string) *Agent {
	agent, ok := agents[serverName]
	if ok {
		return agent
	} else {
		return nil
	}
}

func Destroy() {
	var beginNoRequestTime int64 = 0
	for {
		time.Sleep(time.Second)

		curTime := time.Now().Unix()
		if GetRequestCount() == 0 {
			if beginNoRequestTime == 0 {
				beginNoRequestTime = curTime
				continue
			} else if curTime - beginNoRequestTime >= 5 {
				break
			}
		} else {
			beginNoRequestTime = 0
		}
	}

	if server != nil {
		server.Close()
	}

	for _, client := range clients {
		client.Close()
	}
}

type Agent struct {
	ServerName string
	conn       *network.TCPConn
	userData   interface{}
}

func newAgent(conn *network.TCPConn) network.Agent {
	a := new(Agent)
	a.conn = conn

	msg := &S2S_NotifyServerName{ServerName: conf.ServerName}
	a.WriteMsg(msg)
	return a
}

func (a *Agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			log.Debug("read message: %v", err)
			break
		}

		if Processor != nil {
			msg, err := Processor.Unmarshal(data)
			if err != nil {
				log.Debug("unmarshal message error: %v", err)
				break
			}
			err = Processor.Route(msg, a)
			if err != nil {
				log.Debug("route message error: %v", err)
				break
			}
		}
	}
}

func (a *Agent) OnClose() {}

func (a *Agent) WriteMsg(msg interface{}) {
	if Processor != nil {
		data, err := Processor.Marshal(msg)
		if err != nil {
			log.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
		err = a.conn.WriteMsg(data...)
		if err != nil {
			log.Error("write message %v error: %v", reflect.TypeOf(msg), err)
		}
	}
}

func (a *Agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *Agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *Agent) Close() {
	a.conn.Close()
}

func (a *Agent) Destroy() {
	a.conn.Destroy()
}

func (a *Agent) UserData() interface{} {
	return a.userData
}

func (a *Agent) SetUserData(data interface{}) {
	a.userData = data
}

func (a *Agent) Go(id interface{}, args ...interface{}) {
	msg := &S2S_RequestMsg{MsgID: id, CallType: callNotForResult, Args: args}
	a.WriteMsg(msg)
}

func (a *Agent) Call0(id interface{}, args ...interface{}) error {
	chanSyncRet := make(chan *chanrpc.RetInfo, 1)

	request := &RequestInfo{chanRet: chanSyncRet}
	requestID := registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callForResult, Args: args}
	a.WriteMsg(msg)

	ri := <-chanSyncRet
	return ri.Err
}

func (a *Agent) Call1(id interface{}, args ...interface{}) (interface{}, error) {
	chanSyncRet := make(chan *chanrpc.RetInfo, 1)

	request := &RequestInfo{chanRet: chanSyncRet}
	requestID := registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callForResult, Args: args}
	a.WriteMsg(msg)

	ri := <-chanSyncRet
	return ri.Ret, ri.Err
}

func (a *Agent) CallN(id interface{}, args ...interface{}) ([]interface{}, error) {
	chanSyncRet := make(chan *chanrpc.RetInfo, 1)

	request := &RequestInfo{chanRet: chanSyncRet}
	requestID := registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callForResult, Args: args}
	a.WriteMsg(msg)

	ri := <-chanSyncRet
	return chanrpc.Assert(ri.Ret), ri.Err
}

func (a *Agent) AsynCall(chanAsynRet chan *chanrpc.RetInfo, id interface{}, args ...interface{}) {
	if len(args) < 1 {
		panic(fmt.Sprintf("%v asyn call of callback function not found", id))
	}

	lastIndex := len(args) - 1
	cb := args[lastIndex]
	args = args[:lastIndex]

	var callType uint8
	switch cb.(type) {
	case func(error):
		callType = callForResult
	case func(interface{}, error):
		callType = callForResult
	case func([]interface{}, error):
		callType = callForResult
	default:
		panic(fmt.Sprintf("%v asyn call definition of callback function is invalid", id))
	}

	request := &RequestInfo{cb: cb, chanRet: chanAsynRet}
	requestID := registerRequest(request)
	msg := &S2S_RequestMsg{RequestID: requestID, MsgID: id, CallType: callType, Args: args}
	a.WriteMsg(msg)
}