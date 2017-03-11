package chanrpc

import (
	"errors"
	"fmt"
	"github.com/name5566/leaf/log"
)

// one server per goroutine (goroutine not safe)
// one client per goroutine (goroutine not safe)
type Server struct {
	// id -> function
	//
	// function:
	// func(args []interface{})
	// func(args []interface{}) interface{}
	// func(args []interface{}) []interface{}
	functions map[interface{}]interface{}
	ChanCall  chan *CallInfo
}

type CallInfo struct {
	f       interface{}
	args    []interface{}
	chanRet chan *RetInfo
	cb      interface{}
}

type RetInfo struct {
	// nil
	// interface{}
	// []interface{}
	Ret interface{}
	Err error
	// callback:
	// func(Err error)
	// func(Ret interface{}, Err error)
	// func(Ret []interface{}, Err error)
	Cb interface{}
}

type ExternalRetFunc func(ret interface{}, err error)
type GetExternalRetFunc func() ExternalRetFunc

type Client struct {
	s               *Server
	ChanSyncRet     chan *RetInfo
	ChanAsynRet     chan *RetInfo
	pendingAsynCall int
}

func NewServer(l int) *Server {
	s := new(Server)
	s.functions = make(map[interface{}]interface{})
	s.ChanCall = make(chan *CallInfo, l)
	return s
}

func Assert(i interface{}) []interface{} {
	if i == nil {
		return nil
	} else {
		return i.([]interface{})
	}
}

// you must call the function before calling Open and Go
func (s *Server) Register(id interface{}, f interface{}) {
	switch f.(type) {
	case func([]interface{}):
	case func([]interface{}) error:
	case func([]interface{}) (interface{}, error):
	case func([]interface{}) ([]interface{}, error):
	default:
		panic(fmt.Sprintf("function id %v: definition of function is invalid", id))
	}

	if _, ok := s.functions[id]; ok {
		panic(fmt.Sprintf("function id %v: already registered", id))
	}

	s.functions[id] = f
}

func (s *Server) ret(ci *CallInfo, ri *RetInfo) (err error) {
	if ci.chanRet == nil {
		if ci.cb != nil {
			ci.cb.(func(*RetInfo))(ri)
		}
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Recover(r)
		}
	}()

	ri.Cb = ci.cb
	ci.chanRet <- ri
	return
}

func (s *Server) exec(ci *CallInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Recover(r)
			s.ret(ci, &RetInfo{Err: fmt.Errorf("%v", r)})
		}
	}()

	isExternalRet := false
	var getRetFunc GetExternalRetFunc = func() ExternalRetFunc {
		isExternalRet = true
		return func(ret interface{}, err error) {
			err = s.ret(ci, &RetInfo{Ret: ret, Err: err})
			if err != nil {
				log.Error("external run return is error: %v", err)
			}
		}
	}
	ci.args = append(ci.args, getRetFunc)

	// execute
	retInfo := &RetInfo{}
	switch ci.f.(type) {
	case func([]interface{}):
		ci.f.(func([]interface{}))(ci.args)
	case func([]interface{}) error:
		retInfo.Err = ci.f.(func([]interface{}) error)(ci.args)
	case func([]interface{}) (interface{}, error):
		retInfo.Ret, retInfo.Err = ci.f.(func([]interface{}) (interface{}, error))(ci.args)
	case func([]interface{}) ([]interface{}, error):
		retInfo.Ret, retInfo.Err = ci.f.(func([]interface{}) ([]interface{}, error))(ci.args)
	default:
		panic("bug")
	}

	if !isExternalRet {
		return s.ret(ci, retInfo)
	}
	return
}

func (s *Server) Exec(ci *CallInfo) {
	err := s.exec(ci)
	if err != nil {
		log.Error("%v", err)
	}
}

// goroutine safe
func (s *Server) Go(id interface{}, args ...interface{}) {
	f := s.functions[id]
	if f == nil {
		log.Error("function id %v: function not registered", id)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Recover(r)
		}
	}()

	s.ChanCall <- &CallInfo{
		f:    f,
		args: args,
	}
}

// goroutine safe
func (s *Server) Call0(id interface{}, args ...interface{}) error {
	return s.Open(0).Call0(id, args...)
}

// goroutine safe
func (s *Server) Call1(id interface{}, args ...interface{}) (interface{}, error) {
	return s.Open(0).Call1(id, args...)
}

// goroutine safe
func (s *Server) CallN(id interface{}, args ...interface{}) ([]interface{}, error) {
	return s.Open(0).CallN(id, args...)
}

func (s *Server) Close() {
	close(s.ChanCall)

	for ci := range s.ChanCall {
		s.ret(ci, &RetInfo{
			Err: errors.New("chanrpc server closed"),
		})
	}
}

// goroutine safe
func (s *Server) Open(l int) *Client {
	c := NewClient(l)
	c.Attach(s)
	return c
}

func NewClient(l int) *Client {
	c := new(Client)
	c.ChanSyncRet = make(chan *RetInfo, 1)
	c.ChanAsynRet = make(chan *RetInfo, l)
	return c
}

func (c *Client) Attach(s *Server) {
	c.s = s
}

func (c *Client) GetServer() *Server {
	return c.s
}

func (c *Client) call(ci *CallInfo, block bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Recover(r)
			err = fmt.Errorf("%v", r)
		}
	}()

	if block {
		c.s.ChanCall <- ci
	} else {
		select {
		case c.s.ChanCall <- ci:
		default:
			err = errors.New("chanrpc channel full")
		}
	}
	return
}

func (c *Client) f(id interface{}, n int) (f interface{}, err error) {
	if c.s == nil {
		err = errors.New("server not attached")
		return
	}

	f = c.s.functions[id]
	if f == nil {
		err = fmt.Errorf("function id %v: function not registered", id)
		return
	}

	var ok bool
	switch n {
	case 0:
		_, ok = f.(func([]interface{}) error)
	case 1:
		_, ok = f.(func([]interface{}) (interface{}, error))
	case 2:
		_, ok = f.(func([]interface{}) ([]interface{}, error))
	default:
		panic("bug")
	}

	if !ok {
		err = fmt.Errorf("function id %v: return type mismatch", id)
	}
	return
}

func (c *Client) Call0(id interface{}, args ...interface{}) error {
	f, err := c.f(id, 0)
	if err != nil {
		return err
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.ChanSyncRet,
	}, true)
	if err != nil {
		return err
	}

	ri := <-c.ChanSyncRet
	return ri.Err
}

func (c *Client) Call1(id interface{}, args ...interface{}) (interface{}, error) {
	f, err := c.f(id, 1)
	if err != nil {
		return nil, err
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.ChanSyncRet,
	}, true)
	if err != nil {
		return nil, err
	}

	ri := <-c.ChanSyncRet
	return ri.Ret, ri.Err
}

func (c *Client) CallN(id interface{}, args ...interface{}) ([]interface{}, error) {
	f, err := c.f(id, 2)
	if err != nil {
		return nil, err
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.ChanSyncRet,
	}, true)
	if err != nil {
		return nil, err
	}

	ri := <-c.ChanSyncRet
	return Assert(ri.Ret), ri.Err
}

func (c *Client) RpcCall(id interface{}, args ...interface{}) {
	if len(args) < 1 {
		panic("callback function not found")
	}

	lastIndex := len(args) - 1
	cb := args[lastIndex]
	args = args[:lastIndex]

	var err error
	f := c.s.functions[id]
	if f == nil {
		err = fmt.Errorf("function id %v: function not registered", id)
		return
	}

	var cbFunc func(*RetInfo)
	if cb != nil {
		cbFunc = cb.(func(*RetInfo))
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		cb:      cb,
	}, false)
	if err != nil && cbFunc != nil {
		cbFunc(&RetInfo{Ret:nil, Err:err})
	}
}

func (c *Client) asynCall(id interface{}, args []interface{}, cb interface{}, n int) {
	f, err := c.f(id, n)
	if err != nil {
		c.ChanAsynRet <- &RetInfo{Err: err, Cb: cb}
		return
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.ChanAsynRet,
		cb:      cb,
	}, false)
	if err != nil {
		c.ChanAsynRet <- &RetInfo{Err: err, Cb: cb}
		return
	}
}

func (c *Client) AsynCall(id interface{}, _args ...interface{}) {
	if len(_args) < 1 {
		panic("callback function not found")
	}

	args := _args[:len(_args)-1]
	cb := _args[len(_args)-1]

	var n int
	switch cb.(type) {
	case func(error):
		n = 0
	case func(interface{}, error):
		n = 1
	case func([]interface{}, error):
		n = 2
	default:
		panic("definition of callback function is invalid")
	}

	// too many calls
	if c.pendingAsynCall >= cap(c.ChanAsynRet) {
		execCb(&RetInfo{Err: errors.New("too many calls"), Cb: cb})
		return
	}

	c.asynCall(id, args, cb, n)
	c.pendingAsynCall++
}

func execCb(ri *RetInfo) {
	defer func() {
		if r := recover(); r != nil {
			log.Recover(r)
		}
	}()

	// execute
	switch ri.Cb.(type) {
	case func(error):
		ri.Cb.(func(error))(ri.Err)
	case func(interface{}, error):
		ri.Cb.(func(interface{}, error))(ri.Ret, ri.Err)
	case func([]interface{}, error):
		ri.Cb.(func([]interface{}, error))(Assert(ri.Ret), ri.Err)
	default:
		panic("bug")
	}
	return
}

func (c *Client) Cb(ri *RetInfo) {
	c.pendingAsynCall--
	execCb(ri)
}

func (c *Client) Close() {
	for c.pendingAsynCall > 0 {
		c.Cb(<-c.ChanAsynRet)
	}
}

func (c *Client) Idle() bool {
	return c.pendingAsynCall == 0
}
