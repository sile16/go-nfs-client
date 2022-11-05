// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
package rpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type tcpTransport struct {
	r       io.Reader
	wc      net.Conn
	timeout time.Duration

	rlock, wlock sync.Mutex
}

// Get the response from the conn, buffer the contents, and return a reader to
// it.
func (t *tcpTransport) recv(recv_buf []byte) (error) {
	t.rlock.Lock()
	defer t.rlock.Unlock()
	if t.timeout != 0 {
		deadline := time.Now().Add(t.timeout)
		t.wc.SetReadDeadline(deadline)
	}

	// Read just first 32 bytes, to get RPC length.
	var hdr uint32
	if err := binary.Read(t.r, binary.BigEndian, &hdr); err != nil {
		return err
	}

	// Old buf allocation, added to the struct so not allocated on every call.
	//buf := make([]byte, hdr&0x7fffffff) 
	//assert(len(buf) < 1024*1024, "RPC response too large")
	rpc_len := hdr&0x7fffffff
	if int(rpc_len) > len(recv_buf) {
		return fmt.Errorf("RPC response too large, buffer: %d, response: %d", len(recv_buf), rpc_len)
	}

	if _, err := io.ReadFull(t.r, recv_buf[0:rpc_len]); err != nil {
		return err
	}
	
	return nil

}

func (t *tcpTransport) Write(buf []byte) (int, error) {
	t.wlock.Lock()
	defer t.wlock.Unlock()

	var hdr uint32 = uint32(len(buf)) | 0x80000000
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, hdr)
	if t.timeout != 0 {
		deadline := time.Now().Add(t.timeout)
		t.wc.SetWriteDeadline(deadline)
	}
	n, err := t.wc.Write(append(b, buf...))

	return n, err
}

func (t *tcpTransport) Close() error {
	return t.wc.Close()
}

func (t *tcpTransport) SetTimeout(d time.Duration) {
	t.timeout = d
	if d == 0 {
		var zeroTime time.Time
		t.wc.SetDeadline(zeroTime)
	}
}
