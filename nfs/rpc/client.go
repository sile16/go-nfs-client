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
	rpc_lock sync.Mutex
	rpc_depth int // number of outstanding RPCs

	// buffer pool for replies
	replyPool *sync.Pool
	buf []byte	// buffer to hold the response
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


	return &Client{tcpTransport: t, 
		           rpc_depth: rpc_depth, 
				   replyPool: &bufPool}, 
				   nil
}

type message struct {
	Xid     uint32
	Msgtype uint32
	Body    interface{}
}

func (c *Client) Call(call interface{}) (io.ReadSeeker, error) {
	retries := 1
	
	// Create the RPC Message.
	msg := &message{
		Xid:  atomic.AddUint32(&xid, 1),
		Body: call,
	}

	// Write that msg into a buffer using XDR 
	w := new(bytes.Buffer)
	if err := xdr.Write(w, msg); err != nil {
		return nil, err
	}

retry:

	read_buf := c.replyPool.Get().(*[]byte)
	defer c.replyPool.Put(read_buf)
	c.rpc_lock.Lock()

	// Start our read in a go routine, RPC replies should be delivered in order.
	err_chan := make(chan error)
	go func() {
    	err := c.recv(*read_buf)
		err_chan <- err
	}()

	// Write the message to the wire.
	_, err := c.Write(w.Bytes()); 

	// We can unlock, but we are alread in the queue for the read lock
	// and we want to release this lock so others can write while we are reading.
	c.rpc_lock.Unlock()

	if err != nil {
		return nil, err
	}
	
	// now we wait for our read to finish.
	err = <- err_chan 
	if err != nil {
		return nil, err
	}
	res := bytes.NewReader(*read_buf)

	xid, err := xdr.ReadUint32(res)
	if err != nil {
		return nil, err
	}

	if xid != msg.Xid {
		return nil, fmt.Errorf("xid did not match, expected: %x, received: %x", msg.Xid, xid)
	}

	mtype, err := xdr.ReadUint32(res)
	if err != nil {
		return nil, err
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
			return res, nil
		case ProgUnavail:
			return nil, fmt.Errorf("rpc: PROG_UNAVAIL - server does not recognize the program number")
		case ProgMismatch:
			return nil, fmt.Errorf("rpc: PROG_MISMATCH - program version does not exist on the server")
		case ProcUnavail:
			return nil, fmt.Errorf("rpc: PROC_UNAVAIL - unrecognized procedure number")
		case GarbageArgs:
			// emulate Linux behaviour for GARBAGE_ARGS
			if retries > 0 {
				util.Debugf("Retrying on GARBAGE_ARGS per linux semantics")
				retries--
				goto retry
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
