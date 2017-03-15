package gob

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/name5566/leaf/chanrpc"
	"github.com/name5566/leaf/log"
	"reflect"
	"bytes"
)

type Processor struct {
	encoders chan *Encoder
	decoders chan *Decoder
	msgInfo  map[string]*MsgInfo
}

type Buffer struct {
	*bytes.Buffer
}

type Encoder struct {
	buffer *bytes.Buffer
	coder  *gob.Encoder
}

type Decoder struct {
	buffer *Buffer
	coder  *gob.Decoder
}

type MsgInfo struct {
	msgType       reflect.Type
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

type MsgHandler func([]interface{})

type MsgRaw struct {
	msgID      string
	msgRawData []byte
}

func NewProcessor(coderPoolSize int) *Processor {
	p := new(Processor)
	p.encoders = make(chan *Encoder, coderPoolSize)
	p.decoders = make(chan *Decoder, coderPoolSize)
	for i := 0; i < coderPoolSize; i++ {
		encoderBuff := &bytes.Buffer{}
		encoder := gob.NewEncoder(encoderBuff)
		p.encoders <- &Encoder{encoderBuff, encoder}

		decoderBuff := &Buffer{}
		decoder := gob.NewDecoder(decoderBuff)
		p.decoders <- &Decoder{decoderBuff, decoder}
	}

	p.msgInfo = make(map[string]*MsgInfo)
	return p
}

func (p *Processor) popEncoder() *Encoder {
	coder := <-p.encoders
	coder.buffer.Reset()
	return coder
}

func (p *Processor) pushEncoder(coder *Encoder) {
	p.encoders <- coder
}

func (p *Processor) popDecoder(data []byte) *Decoder {
	coder := <-p.decoders
	coder.buffer.Buffer = bytes.NewBuffer(data)
	return coder
}

func (p *Processor) pushDecoder(coder *Decoder) {
	p.decoders <- coder
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) Register(msg interface{}) string {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("gob message pointer required")
	}
	msgID := msgType.Elem().Name()
	if msgID == "" {
		log.Fatal("unnamed gob message")
	}
	if _, ok := p.msgInfo[msgID]; ok {
		log.Fatal("message %v is already registered", msgID)
	}

	i := new(MsgInfo)
	i.msgType = msgType
	p.msgInfo[msgID] = i

	for i := 0; i < len(p.decoders); i++ {
		data, _ := p.Marshal(msg)
		p.Unmarshal(data[0])
	}
	return msgID
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRouter(msg interface{}, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("gob message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		log.Fatal("message %v not registered", msgID)
	}

	i.msgRouter = msgRouter
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetHandler(msg interface{}, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("gob message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		log.Fatal("message %v not registered", msgID)
	}

	i.msgHandler = msgHandler
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRawHandler(msg interface{}, msgRawHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		log.Fatal("message %v not registered", msgID)
	}

	i.msgRawHandler = msgRawHandler
}

// goroutine safe
func (p *Processor) Route(msg interface{}, userData interface{}) error {
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		i, ok := p.msgInfo[msgRaw.msgID]
		if !ok {
			return fmt.Errorf("message %v not registered", msgRaw.msgID)
		}
		if i.msgRawHandler != nil {
			i.msgRawHandler([]interface{}{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// json
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return errors.New("gob message pointer required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		return fmt.Errorf("message %v not registered", msgID)
	}
	if i.msgHandler != nil {
		i.msgHandler([]interface{}{msg, userData})
	}
	if i.msgRouter != nil {
		i.msgRouter.Go(msgType, msg, userData)
	} else if i.msgHandler == nil {
		log.Error("%v msg without any handler", msgID)
	}
	return nil
}

// goroutine safe
func (p *Processor) Unmarshal(data []byte) (interface{}, error) {
	dec := p.popDecoder(data)
	defer p.pushDecoder(dec)

	var msgID string
	err := dec.coder.Decode(&msgID)
	if err != nil {
		return nil, err
	}

	i, ok := p.msgInfo[msgID]
	if !ok {
		return nil, fmt.Errorf("message %v not registered", msgID)
	}

	// msg
	if i.msgRawHandler != nil {
		return MsgRaw{msgID, dec.buffer.Bytes()}, nil
	} else {
		msg := reflect.New(i.msgType.Elem()).Interface()
		return msg, dec.coder.Decode(msg)
	}

	panic("bug")
}

// goroutine safe
func (p *Processor) Marshal(msg interface{}) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return nil, errors.New("json message pointer required")
	}
	msgID := msgType.Elem().Name()
	if _, ok := p.msgInfo[msgID]; !ok {
		return nil, fmt.Errorf("message %v not registered", msgID)
	}

	// data
	enc := p.popEncoder()
	defer p.pushEncoder(enc)

	err := enc.coder.Encode(&msgID)
	if err != nil {
		return [][]byte{enc.buffer.Bytes()}, err
	}

	err = enc.coder.Encode(msg)
	return [][]byte{enc.buffer.Bytes()}, err
}
