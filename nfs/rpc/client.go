// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
package rpc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sile16/go-nfs-client/nfs/util"
	"github.com/sile16/go-nfs-client/nfs/xdr"
)

const (
	MsgAccepted = iota
	MsgDenied
)

const (
	Success = iota
	ProgUnavail
	ProgMismatch
	ProcUnavail
	GarbageArgs
	SystemErr
)

const (
	RpcMismatch = iota
)

var xid uint32

func init() {
	// seed the XID (which is set by the client)
	xid = rand.New(rand.NewSource(time.Now().UnixNano())).Uint32()
}

type Client struct {
	*tcpTransport
	reqMutex sync.Mutex
	mutex sync.Mutex

	Rpc_depth int // number of outstanding RPCs

	// buffer pool for replies
	replyPool *sync.Pool
	buf []byte	// buffer to hold the response
	pending map[uint32]*Rpc_call
	closing bool
	shutdown bool
}

func DialTCP(network string, ldr *net.TCPAddr, addr string) (*Client, error) {
	return DialTCPDepth(network, ldr, addr, 1)
}

func DialTCPDepth(network string, ldr *net.TCPAddr, addr string, rpc_depth int) (*Client, error) {
	a, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP(a.Network(), ldr, a)
	if err != nil {
		return nil, err
	}

	t := &tcpTransport{
		r:  bufio.NewReader(conn),
		wc: conn,
	}

	// allocate buffers for replies
	var bufPool = sync.Pool{
		New: func() any {
			// The Pool's New function should generally only return pointer
			// types, since a pointer can be put into the return interface
			// value without an allocation:
			buf := make([]byte, 520*1024) // 512K + extra for RPC headers
			return &buf
		},
	}

	client := &Client{
		tcpTransport: t,
		reqMutex:     sync.Mutex{},
		Rpc_depth:    rpc_depth,
		replyPool:    &bufPool,
		buf:          []byte{},
		pending:      make(map[uint32]*Rpc_call),
	}
	// start the receiver loop
	go client.input()

	return client, nil
}

type Rpc_call struct {
	msg *Message
	msg_bytes    []byte

	Res io.ReadSeeker
	Done chan *Rpc_call
	Error error
	retries int
}

type Message struct {
	Xid     uint32
	Msgtype uint32
	Body    interface{}
}

func (call *Rpc_call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		// We don't want to block here. It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		util.Debugf("rpc: discarding Call reply due to insufficient Done chan capacity")
	}
}

// Close calls the underlying codec's Close method. If the connection is already
// shutting down, ErrShutdown is returned.
func (client *Client) Close() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.closing {

		return fmt.Errorf("rpc: client is shutting down")
	}
	client.closing = true
	return nil
}

func (c *Client) send(call *Rpc_call) {

	c.reqMutex.Lock()
	defer c.reqMutex.Unlock()

	// Register this call.
	c.mutex.Lock()
	if c.shutdown || c.closing {
		c.mutex.Unlock()
		call.Error = fmt.Errorf("rpc: client is shutting down")
		call.done()
		return
	}
	seq := call.msg.Xid
	c.pending[call.msg.Xid] = call
	c.mutex.Unlock()

	// Send the request.
	_, err := c.Write(call.msg_bytes)
	if err != nil {
		c.mutex.Lock()
		// Why do we have this line here? 
		call = c.pending[seq]
		delete(c.pending, seq)
		c.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}


// we run this in a loop in a separate goroutine
func (c *Client) input() {
	var err error
	var res io.ReadSeeker

	for err == nil {

		// read next rpc call from the wire.
		call, err := c.readRPCCall()
		if err != nil {
			break
		}

		switch {
		case call == nil:
			// We've got no pending call. That usually means that
			// WriteRequest partially failed, and call was already
			// removed; response is a server telling us about an
			// error reading request body. We should still attempt
			// to read error body, but there's no one to give it to.
			
		default:
			// We've got an error response. Give this to the request;
			// any subsequent requests will get the ReadResponseBody
			// error if there is one.
			call.Error = err
			call.Res = res
			call.done()
		}
	}
	// Terminate pending calls.
	c.reqMutex.Lock()
	c.mutex.Lock()
	c.shutdown = true
	closing := c.closing
	if err == io.EOF {
		if closing {
			err = fmt.Errorf("rpc: client protocol closing")
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
	c.mutex.Unlock()
	c.reqMutex.Unlock()
	if err != io.EOF && !closing {
		util.Debugf("rpc: client protocol error:", err)
	}
}

func (c *Client) readRPCCall() ( *Rpc_call, error) {
	// todo can we make a pool of buffers for this?
	// read the entire message from the wire
	// read from network,
	res, err := c.tcpTransport.recv()
	if err != nil {
		return nil, err
	}

	// get the seq number
	seq, err := xdr.ReadUint32(res)
	if err != nil {
		return nil, err
	}

	// lookup call by the seq / xid
	c.mutex.Lock()
	call := c.pending[seq]
	delete(c.pending, seq)
	c.mutex.Unlock()

	if call == nil {
		// received a response for a call we don't know about
		return nil, fmt.Errorf("rpc: received response for unknown call %d", seq)
	}

	mtype,  err := xdr.ReadUint32(res)
	if err != nil {
		return call, err
	}

	if mtype != 1 {
		return nil, fmt.Errorf("message as not a reply: %d", mtype)
	}

	status, err := xdr.ReadUint32(res)
	if err != nil {
		return nil, err
	}

	switch status {
	case MsgAccepted:

		// padding
		_, err = xdr.ReadUint32(res)
		if err != nil {
			panic(err.Error())
		}

		opaque_len, err := xdr.ReadUint32(res)
		if err != nil {
			panic(err.Error())
		}

		_, err = res.Seek(int64(opaque_len), io.SeekCurrent)
		if err != nil {
			panic(err.Error())
		}

		acceptStatus, _ := xdr.ReadUint32(res)

		switch acceptStatus {
		case Success:
			call.Res = res
			return call, nil
		case ProgUnavail:
			return nil, fmt.Errorf("rpc: PROG_UNAVAIL - server does not recognize the program number")
		case ProgMismatch:
			return nil, fmt.Errorf("rpc: PROG_MISMATCH - program version does not exist on the server")
		case ProcUnavail:
			return nil, fmt.Errorf("rpc: PROC_UNAVAIL - unrecognized procedure number")
		case GarbageArgs:
			// emulate Linux behaviour for GARBAGE_ARGS
			if call.retries == 0 {
				// try to send the message again
				// and reprocess call
				util.Debugf("Retrying on GARBAGE_ARGS per linux semantics")
				call.retries++
				c.send(call)
				call.Error = fmt.Errorf("rpc: GARBAGE_ARGS - server could not decode arguments, sending again")
				return call, nil
			}

			return nil, fmt.Errorf("rpc: GARBAGE_ARGS - rpc arguments cannot be XDR decoded")
		case SystemErr:
			return nil, fmt.Errorf("rpc: SYSTEM_ERR - unknown error on server")
		default:
			return nil, fmt.Errorf("rpc: unknown accepted status error: %d", acceptStatus)
		}

	case MsgDenied:
		rejectStatus, _ := xdr.ReadUint32(res)
		switch rejectStatus {
		case RpcMismatch:

		default:
			return nil, fmt.Errorf("rejectedStatus was not valid: %d", rejectStatus)
		}

	default:
		return nil, fmt.Errorf("rejectedStatus was not valid: %d", status)
	}

	panic("unreachable")

}

func newRPCCall(call interface{}) (*Rpc_call, error) {
	// Create the RPC Message.
	msg := &Message{
		Xid:  atomic.AddUint32(&xid, 1),
		Body: call,
	}

	// Write that msg into a buffer using XDR 
	w := new(bytes.Buffer)
	err := xdr.Write(w, msg); if err != nil {
		return nil, err
	}

	return &Rpc_call{
		msg: msg,
		msg_bytes: w.Bytes(),
	}, nil
}

// Go invokes the function asynchronously. It returns the Call structure representing
// the invocation. The done channel will signal when the call is complete by returning
// the same Call object. If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
func (client *Client) Go(call interface{}, done chan *Rpc_call) *Rpc_call {
	rpccall, err := newRPCCall(call)
	if err != nil {
		return nil
	}

	if done == nil {
		done = make(chan *Rpc_call, 10) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			panic("rpc: done channel is unbuffered")
		}
	}
	rpccall.Done = done
	client.send(rpccall)
	return rpccall
}

func (c *Client) Call(call interface{}) (io.ReadSeeker, error) {
	rpccall := <-c.Go(call, make(chan *Rpc_call, 1)).Done
	return rpccall.Res, rpccall.Error
}