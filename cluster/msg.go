package cluster

import (
	"github.com/name5566/leaf/network/json"
	"errors"
	"fmt"
	"github.com/name5566/leaf/chanrpc"
	"github.com/name5566/leaf/log"
)

var (
	Processor = json.NewProcessor()
)

type S2S_NotifyServerName struct {
	ServerName	string
}

const (
	callGo = iota
	call0
	call1
	callN
)

type S2S_RequestMsg struct {
	RequestID uint32
	MsgID     interface{}
	CallType  uint8
	Args      []interface{}
}

type S2S_ResponseMsg struct {
	RequestID uint32
	Ret       interface{}
	Err       error
}

func handleNotifyServerName(args []interface{}) {
	msg := args[0].(*S2S_NotifyServerName)
	agent := args[1].(*Agent)
	agent.ServerName = msg.ServerName
	agents[agent.ServerName] = agent
}

func handleRequestMsg(args []interface{}) {
	recvMsg := args[0].(*S2S_RequestMsg)
	agent := args[1].(*Agent)

	msgID := recvMsg.MsgID
	sendMsg := &S2S_ResponseMsg{RequestID: recvMsg.RequestID}
	server, ok := routeMap[msgID]
	if !ok {
		sendMsg.Err = errors.New(fmt.Sprintf("%v msg is not set route", msgID))
		agent.WriteMsg(sendMsg)
		return
	}

	args = recvMsg.Args
	switch recvMsg.CallType {
	case callGo:
		server.Go(msgID, args...)
	case call0:
		go func() {
			sendMsg.Err = server.Call0(msgID, args...)
			agent.WriteMsg(sendMsg)
		}()
	case call1:
		go func() {
			sendMsg.Ret, sendMsg.Err = server.Call1(msgID, args...)
			agent.WriteMsg(sendMsg)
		}()
	case callN:
		go func() {
			sendMsg.Ret, sendMsg.Err = server.CallN(msgID, args...)
			agent.WriteMsg(sendMsg)
		}()
	}
}

func handleResponseMsg(args []interface{}) {
	msg := args[0].(*S2S_ResponseMsg)
	request := popRequest(msg.RequestID)
	if request == nil {
		log.Error("request id %v is not exist", msg.RequestID)
	}

	ret := &chanrpc.RetInfo{Ret: msg.Ret, Err: msg.Err, Cb: request.cb}
	request.chanRet <- ret
}

func init() {
	Processor.SetHandler(&S2S_NotifyServerName{}, handleNotifyServerName)
	Processor.SetHandler(&S2S_RequestMsg{}, handleRequestMsg)
	Processor.SetHandler(&S2S_ResponseMsg{}, handleResponseMsg)
}