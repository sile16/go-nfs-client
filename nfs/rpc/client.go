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

	"github.com/sile16/go-nfs-client/nfs/metrics"
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
	mutex    sync.Mutex

	Rpc_depth int // number of outstanding RPCs

	// buffer pool for replies
	//replyPool *sync.Pool
	buf      []byte // buffer to hold the response
	pending  map[uint32]*Rpc_call
	closing  bool
	shutdown bool

	input_running atomic.Bool
}

//func DialTCP(network string, ldr *net.TCPAddr, addr string) (*Client, error) {
//	return DialTCPDepth(network, ldr, addr, 8)
//}

func DialTCP(network string, ldr *net.TCPAddr, addr string) (*Client, error) {
	util.Debugf("rpc: DialTCP: %s", addr)
	a, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP(a.Network(), ldr, a)
	util.Debugf("net.DialTCP: %v", conn)
	if err != nil {
		return nil, err
	}

	t := &tcpTransport{
		r:  bufio.NewReader(conn),
		wc: conn,
	}

	// allocate buffers for replies
	/*var bufPool = sync.Pool{
		New: func() any {
			// The Pool's New function should generally only return pointer
			// types, since a pointer can be put into the return interface
			// value without an allocation:
			buf := make([]byte, 520*1024) // 512K + extra for RPC headers
			return &buf
		},
	}*/

	client := &Client{
		tcpTransport: t,
		reqMutex:     sync.Mutex{},
		//replyPool:    &bufPool,
		buf:     []byte{},
		pending: make(map[uint32]*Rpc_call),
	}
	// start the receiver loop
	metrics.Monitored_addrs = append(metrics.Monitored_addrs, a)
	go client.input()

	return client, nil
}

type Rpc_call struct {
	Msg           *Message
	Msg_bytes     []byte
	send_time     time.Time
	response_time time.Time

	Res       io.ReadSeeker
	Res_bytes []byte
	DoneChan  chan *Rpc_call
	Error     error
	retries   int
}

type Message struct {
	Xid     uint32
	Msgtype uint32
	Body    interface{}
}

func (call *Rpc_call) done() {
	// call is only sent into the channel if the channel is not full
	select {
	case call.DoneChan <- call:
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
	util.Debugf("rpc: client is closing")
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.closing {

		return fmt.Errorf("rpc: client is shutting down")
	}

	client.tcpTransport.Close()

	client.closing = true
	return nil
}

func (c *Client) send(call *Rpc_call) {
	util.Debugf("rpc: sending call %v", call.Msg)

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
	seq := call.Msg.Xid
	c.pending[call.Msg.Xid] = call
	c.mutex.Unlock()

	// Send the request.
	_, err := c.Write(call.Msg_bytes)
	metrics.RpcOutstandingRequests.Inc()
	metrics.RpcRequestsCounter.Inc()

	call.send_time = time.Now()
	util.Debugf("rpc: sent call %v", call.Msg)
	if err != nil {
		c.mutex.Lock()
		// If we error, it means the send is failed and we should't wait for the response.
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
// Input loops onh readRPCCall and then calls done() on each one which puts it into the reply chan.
func (c *Client) input() {
	var err error

	util.Debugf("rpc: client input loop starting")

	if c.input_running.Swap(true) {
		util.Debugf("rpc: client input already running, returning")
		return
	}

	prev_xid := uint32(0)

	for err == nil {

		// read next rpc call from the wire, this will block when no data is available
		call, err := c.readRPCCall()
		if call != nil {
			call.response_time = time.Now()
			metrics.RpcRequestLatencyuS.Observe(float64(call.response_time.Sub(call.send_time).Microseconds()))
		}

		if call == nil {
			util.Debugf("rpc: client input loop got nil call")
		}

		metrics.RpcOutstandingRequests.Dec()

		if err != nil {
			break
		}

		// check to see if server responds to RPCs out of order.
		if call.Msg != nil {
			if prev_xid != 0 && prev_xid+1 != call.Msg.Xid {
				util.Infof("rpc: client input loop detected xid out of order, prev_xid: %d, xid: %d", prev_xid, call.Msg.Xid)
			}
			prev_xid = call.Msg.Xid
		}

		switch {
		case call == nil:
			// We've got no pending call. That usually means that
			// WriteRequest partially failed, and call was already
			// removed; response is a server telling us about an
			// error reading request body. We should still attempt
			// to read error body, but there's no one to give it to.
			util.Debugf("rpc: client input received for unknown call.")

		default:
			// We've got an error response. Give this to the request;
			// any subsequent requests will get the ReadResponseBody
			// error if there is one.

			call.Error = err
			//call.Res = res
			call.done()
		}
	}

	util.Debugf("rpc: input loop exiting")
	// Terminate pending calls.
	c.reqMutex.Lock()
	defer c.reqMutex.Unlock()
	c.mutex.Lock()
	defer c.mutex.Unlock()
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

	if err != io.EOF && !closing {
		util.Debugf("rpc: client protocol error:", err)
	}
}

func (c *Client) readRPCCall() (*Rpc_call, error) {
	// todo can we make a pool of buffers for this?
	// read the entire message from the wire
	// read from network,
	//util.Debugf("rpc: client readRPCCall reading next call")
	res_bytes, err := c.tcpTransport.recv()
	res := bytes.NewReader(res_bytes)
	if err != nil {
		return nil, err
	}

	// get the seq number
	//	util.Debugf("rpc: client input got a response")
	seq, err := xdr.ReadUint32(res)
	if err != nil {
		return nil, err
	}

	// lookup call by the seq / xid
	c.mutex.Lock()
	call := c.pending[seq]
	delete(c.pending, seq)
	c.mutex.Unlock()

	util.Debugf("rpc: client input got a response for call %d", seq)
	mtype, err := xdr.ReadUint32(res)
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
			call.Res_bytes = res_bytes[24+opaque_len:]
			return call, nil
		case ProgUnavail:
			return call, fmt.Errorf("rpc: PROG_UNAVAIL - server does not recognize the program number")
		case ProgMismatch:
			return call, fmt.Errorf("rpc: PROG_MISMATCH - program version does not exist on the server")
		case ProcUnavail:
			return call, fmt.Errorf("rpc: PROC_UNAVAIL - unrecognized procedure number")
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

			return call, fmt.Errorf("rpc: GARBAGE_ARGS - rpc arguments cannot be XDR decoded")
		case SystemErr:
			return call, fmt.Errorf("rpc: SYSTEM_ERR - unknown error on server")
		default:
			return call, fmt.Errorf("rpc: unknown accepted status error: %d", acceptStatus)
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
	err := xdr.Write(w, msg)
	if err != nil {
		return nil, err
	}

	//todo w.Bytes() , do we need that?
	return &Rpc_call{
		Msg:       msg,
		Msg_bytes: w.Bytes(),
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
		done = make(chan *Rpc_call, 1) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			panic("rpc: done channel is unbuffered")
		}
	}
	rpccall.DoneChan = done
	client.send(rpccall)
	return rpccall
}

func (c *Client) Call(call interface{}) (io.ReadSeeker, error) {
	rpccall_chan := c.Go(call, make(chan *Rpc_call, 1)).DoneChan
	rpccall := <-rpccall_chan
	return rpccall.Res, rpccall.Error
}
