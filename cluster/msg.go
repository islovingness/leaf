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
	callNotForResult  = iota
	callForResult
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
	client, ok := routeMap[msgID]
	if !ok {
		sendMsg.Err = errors.New(fmt.Sprintf("%v msg is not set route", msgID))
		agent.WriteMsg(sendMsg)
		return
	}

	sendMsgFunc := func(ret *chanrpc.RetInfo) {
		sendMsg.Ret, sendMsg.Err = ret.Ret, ret.Err
		agent.WriteMsg(sendMsg)
	}

	args = recvMsg.Args
	switch recvMsg.CallType {
	case callNotForResult:
		args = append(args, nil)
		client.RpcCall(msgID, args...)
	default:
		args = append(args, sendMsgFunc)
		client.RpcCall(msgID, args...)
	}
}

func handleResponseMsg(args []interface{}) {
	msg := args[0].(*S2S_ResponseMsg)
	request := popRequest(msg.RequestID)
	if request == nil {
		log.Error("request id %v is not exist", msg.RequestID)
		return
	}

	ret := &chanrpc.RetInfo{Ret: msg.Ret, Err: msg.Err, Cb: request.cb}
	request.chanRet <- ret
}

func init() {
	Processor.SetHandler(&S2S_NotifyServerName{}, handleNotifyServerName)
	Processor.SetHandler(&S2S_RequestMsg{}, handleRequestMsg)
	Processor.SetHandler(&S2S_ResponseMsg{}, handleResponseMsg)
}