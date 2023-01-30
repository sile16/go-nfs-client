// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/sile16/go-nfs-client/nfs/rpc"
	"github.com/sile16/go-nfs-client/nfs/util"
	"github.com/sile16/go-nfs-client/nfs/xdr"
)

// File wraps the NfsProc3Read and NfsProc3Write methods to implement a
// io.ReadWriteCloser.
type File struct {
	*Target

	// current position
	curr     uint64
	size     int64
	fsinfo   *FSInfo
	io_depth int
	max_write uint32

	// filehandle to the file
	fh []byte
}

// sets the number of concurrent io operations for sync functions readfrom, & write.
func (f *File) SetIODepth(depth int) {
	f.io_depth = depth
}

// sets the max write size for sync functions readfrom, & write.
func (f *File) SetMaxWriteSize(size uint32) {
	f.max_write = size
}

// Readlink gets the target of a symlink
func (f *File) Readlink() (string, error) {
	type ReadlinkArgs struct {
		rpc.Header
		FH []byte
	}

	type ReadlinkRes struct {
		Attr PostOpAttr
		data []byte
	}

	r, err := f.call(&ReadlinkArgs{
		Header: rpc.Header{
			Rpcvers: 2,
			Prog:    Nfs3Prog,
			Vers:    Nfs3Vers,
			Proc:    NFSProc3Readlink,
			Cred:    f.auth,
			Verf:    rpc.AuthNull,
		},
		FH: f.fh,
	})

	if err != nil {
		util.Debugf("readlink(%x): %s", f.fh, err.Error())
		return "", err
	}

	readlinkres := &ReadlinkRes{}
	if err = xdr.Read(r, readlinkres); err != nil {
		return "", err
	}

	if readlinkres.data, err = xdr.ReadOpaque(r); err != nil {
		return "", err
	}

	return string(readlinkres.data), err
}


// implements the file WriteTo
// will issue 

func (f *File) WriteTo(w io.Writer) (n int64, err error) {

	readSize := f.fsinfo.RTPref
	util.Debugf("read(%x) len=%d offset=%d", f.fh, readSize, f.curr)

	//start a go routine to process the read rpcs responses
	// and write the data to the writer
	// and close the rpc_reply_chan
	rpc_reply_chan := make(chan *rpc.Rpc_call, f.io_depth)
	reply_count_chan := make(chan int, f.io_depth)
	bytes_written := int64(0)

	//channel to pass the data buffers to the write go routine
	data_chan := make(chan []byte, f.io_depth)

	// start a go routine with a buffer channel to write to the writer w
	go func ()  {
		for {
			data := <-data_chan
			n, err := w.Write(data)
			bytes_written = bytes_written + int64(n)
			if err == io.EOF {
				return
			}
			if n != len(data) {
				//todo; handle this error
				return
			}
			<-reply_count_chan
		}
	}()
	

	go func() {
		

		for {
			// buffer to use for the read
			
			rpc_res := <-rpc_reply_chan

			p := make([]byte, readSize)
			f.process_read_response(rpc_res, p)

			// Write the data in p to the writer, check length and error
			data_chan <- p

			if rpc_res.Error == io.EOF {
				return
			}

			if rpc_res.Error != nil {
				util.Debugf("read(%x): %s", f.fh, err.Error())
				return
			}
		}
	}()

	
	// start a go routine to send the read rpcs into the rpc_reply_chan

	go func() {
		for {
			// send the read rpc
			size := min(readSize, uint32(f.size) - uint32(f.curr))
			f.send_read_rpc(size, int(f.curr), rpc_reply_chan)
			reply_count_chan <- 1
			f.curr = f.curr + uint64(size)
			if err != nil {
				//todo; handle this error
				return
			}
		}
	}()

	// wait for the reply_count_chan to be empty
	for {
		if len(reply_count_chan) == 0 {
			break
		}
	}
	return bytes_written, err
} 

// an Async Write function that takes an offset and length and an rpc chan and writes to the file
// at that offset.  It will return a channel that will be closed when the write
// is confirmed by the server.
// limited by the max wsize of the server
// does not update the current position

type WriteArgs struct {
	rpc.Header
	FH     []byte
	Offset uint64
	Count  uint32

	// UNSTABLE(0), DATA_SYNC(1), FILE_SYNC(2) default
	How      uint32
	Contents []byte
}

type WriteRes struct {
	Wcc       WccData
	Count     uint32
	How       uint32
	WriteVerf uint64
}


// implements the file ReadFrom, will readfrom reader and write to the open file.
func (f *File) ReadFrom(r io.Reader) (int64, error) {
	
	chunk_chan := make(chan []byte, f.io_depth)
	rpc_sending_chan := make(chan bool, f.io_depth)
	write_reply_chan := make(chan *WriteRes, f.io_depth)

	util.Debugf("ReadFrom channels created with depth %d", f.io_depth)
	//rpc_reply := make(chan bool, 8)
	//done_chan := make(chan bool, 1)

	var total_rpc_calls atomic.Int32
	var recieved_rpc_repies atomic.Int32
	var all_sent atomic.Bool

	// Read rpc replies
	written_confirmed := uint32(0)
	all_rpc_replies_received := sync.WaitGroup{}
	all_rpc_replies_received.Add(1)

	show_chan_depth := func() {
		util.Debugf("chunk_chan: %d, rpc_pending: %d, unhandled replies: %d",
			len(chunk_chan), total_rpc_calls.Load()-recieved_rpc_repies.Load(), len(write_reply_chan))
	}

	// get rpc write replies
	go func() {
		defer all_rpc_replies_received.Done()
		util.Debugf("NFS File ReadFrom: start liesting for rpc replies.")

		for writeres := range write_reply_chan {
			util.Debugf("NFS File ReadFrom: RPC WriteRes Reply")
			
			//Clear an opent slot in the rpc_sending_chan
			<-rpc_sending_chan
			show_chan_depth()

			written_confirmed += writeres.Count

			util.Debugf("rpc write confirmed no error written=%d total=%d", writeres.Count, written_confirmed)

			recieved_rpc_repies.Add(1)
			if all_sent.Load() &&
				recieved_rpc_repies.Load() == total_rpc_calls.Load() {
				break
			}
		}
	}()

	// Send RPC requests
	//written := uint32(0)
	go func() {
		defer all_sent.Store(true)
		util.Debugf("NFS File ReadFrom: starting to send RPCs")

		max_write_size := f.fsinfo.WTPref
		if f.max_write > 0 {
			max_write_size = min(max_write_size, f.max_write)
		}

		for chunk := range chunk_chan {
			
			totalToWrite := uint32(len(chunk))
			util.Debugf("NFS File ReadFrom: Sending RPC chunk: %d", totalToWrite)

			for written := uint32(0); written < totalToWrite; {
				rpc_sending_chan <- true // will rate limit outstanding RPCs based on buff depth
				show_chan_depth()

				total_rpc_calls.Add(1)
				writeSize := min(max_write_size, totalToWrite-written)
				
				//use the async write call to send the call
				f.WriteAtAsync(chunk[written:written+writeSize], f.curr, write_reply_chan)
				
				f.curr += uint64(writeSize)
				written += uint32(writeSize)
			}
		}
		util.Debugf("NFS File ReadFrom: done sending RPCs")
	}()

	// Read the input stream until EOF
	// on the Read size this is currently only single out standing request unless it's a buffered reader.
	go func() {
		util.Debugf("NFS File ReadFrom: starting to read from input stream")
		chunks_read := 0
		for {
			//todo: a pool of buffers again?
			// We the buffer size to the max write size of the NFS server.

			buf := make([]byte, f.fsinfo.WTPref)
			n, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					if chunks_read == 0 {
						util.Errorf("0 Chunks read from input stream: %s", err.Error())
					}
					break
				}
				util.Errorf("NFS File ReadFrom: read error: %s", err.Error())

			}
			chunks_read++
			util.Debugf("NFS File ReadFrom: Read %d from stream sending to channel", n)
			chunk_chan <- buf[:n]
			show_chan_depth()
		}
		close(chunk_chan)

	}()

	all_rpc_replies_received.Wait()

	return int64(written_confirmed), nil
}

// For reference:
// func (client *Client) Go(call interface{}, done chan *Rpc_call) *Rpc_call {


type ReadArgs struct {
	rpc.Header
	FH     []byte
	Offset uint64
	Count  uint32
}

type ReadRes struct {
	Attr  PostOpAttr
	Count uint32
	EOF   uint32
	Data  struct {
		Length uint32
	}
}


// func (c *Client) Call(call interface{}) (io.ReadSeeker, error) {
//	rpccall_chan := c.Go(call, make(chan *Rpc_call, 1)).Done
//	rpccall := <-rpccall_chan
//	return rpccall.Res, rpccall.Error
//}

// Since we don't know the size of the file, we need to read it in chunks so we don't block
func (f *File) Read(p []byte) (int, error) {

	//todo: have to read until end of file or size of p
	read_response_chan := make(chan *rpc.Rpc_call, 4)

	rpccall_chan := f.ReadAsync(p, int(f.curr), read_response_chan).Done
	rpccall := <-rpccall_chan
	return len(rpccall.Res_bytes), rpccall.Error
}

// Write entire buffer to file, will loop until all data is written
// will issue multiple writes up to the depth in specified DialMount Args
func (f *File) Write(p []byte) (int, error) {
	n, err := f.ReadFrom(bytes.NewReader(p))
	return int(n), err
}

// Close commits the file
func (f *File) Close() error {
	type CommitArg struct {
		rpc.Header
		FH     []byte
		Offset uint64
		Count  uint32
	}

	_, err := f.call(&CommitArg{
		Header: rpc.Header{
			Rpcvers: 2,
			Prog:    Nfs3Prog,
			Vers:    Nfs3Vers,
			Proc:    NFSProc3Commit,
			Cred:    f.auth,
			Verf:    rpc.AuthNull,
		},
		FH: f.fh,
	})

	if err != nil {
		util.Debugf("commit(%x): %s", f.fh, err.Error())
		return err
	}

	return nil
}

// Seek sets the offset for the next Read or Write to offset, interpreted according to whence.
// This method implements Seeker interface.
func (f *File) Seek(offset int64, whence int) (int64, error) {

	// It would be nice to try to validate the offset here.
	// However, as we're working with the shared file system, the file
	// size might even change between NFSPROC3_GETATTR call and
	// Seek() call, so don't even try to validate it.
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return int64(f.curr), errors.New("offset cannot be negative")
		}
		f.curr = uint64(offset)
		return int64(f.curr), nil
	case io.SeekCurrent:
		f.curr = uint64(int64(f.curr) + offset)
		return int64(f.curr), nil
	case io.SeekEnd:
		return f.size, nil
	default:
		// This indicates serious programming error
		return int64(f.curr), errors.New("invalid whence")
	}
}

// OpenFile writes to an existing file or creates one
func (v *Target) OpenFile(path string, perm os.FileMode) (*File, error) {
	_, fh, err := v.Lookup(path)
	if err != nil {
		if os.IsNotExist(err) {
			fh, err = v.Create(path, perm)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	f := &File{
		Target:   v,
		fsinfo:   v.fsinfo,
		fh:       fh,
		io_depth: 2, //default in linux rpc
	}

	return f, nil
}

// Open opens a file for reading
func (v *Target) Open(path string) (*File, error) {
	info, fh, err := v.Lookup(path)
	if err != nil {
		return nil, err
	}

	f := &File{
		Target: v,
		fsinfo: v.fsinfo,
		size:   info.Size(),
		fh:     fh,
	}

	return f, nil
}




func min(x, y uint32) uint32 {
	if x > y {
		return y
	}
	return x
}

//proces read response from rpc call
func (f *File) process_read_response(rpc_res *rpc.Rpc_call, p []byte) {
	r := rpc_res.Res

	readres := &ReadRes{}

	rpc_res.Error = xdr.Read(r, readres)

	if rpc_res.Error != nil {
		return
	}

	var n int 

	n, rpc_res.Error = r.Read(p[:readres.Data.Length])
	if n != int(readres.Data.Length) {
		rpc_res.Error = io.ErrShortBuffer
		return
	}

	if rpc_res.Error != nil {
		return
	}

	if readres.EOF != 0 {
		rpc_res.Error = io.EOF
	}

	if rpc_res.Error != nil {
		util.Debugf("read(%x): %s", f.fh, rpc_res.Error)
		return
	}
}

//send read rpc call, params offset, count and read_done channel
func (f *File) send_read_rpc(count uint32, offset int, read_done chan *rpc.Rpc_call) *rpc.Rpc_call {
	return f.Go(&ReadArgs{
		Header: rpc.Header{
			Rpcvers: 2,
			Prog:    Nfs3Prog,
			Vers:    Nfs3Vers,
			Proc:    NFSProc3Read,
			Cred:    f.auth,
			Verf:    rpc.AuthNull,
		},
		FH:     f.fh,
		Offset: uint64(offset),
		Count:  count,
	}, read_done)
}

// Async read from file
// Does not affect file curr position
func (f *File) ReadAsync(p []byte, offset int, read_done chan *rpc.Rpc_call) *rpc.Rpc_call {

	readSize := min(f.fsinfo.RTPref, uint32(len(p)))
	util.Debugf("read(%x) len=%d offset=%d", f.fh, readSize, f.curr)

	// start the go routine to read the response and process it from an rpc call to a read repsonse
	// worried about how much overhead this is going to add to the read for a new goroutine on each 512KB
	go func() {
		rpc_res := <-read_done
		f.process_read_response(rpc_res, p)
		read_done <- rpc_res
	}()

	return f.send_read_rpc(readSize, offset, read_done)
}

