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

func (f *File) ReadFrom(r io.Reader) (int64, error) {
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

	chunk_chan := make(chan []byte, f.io_depth)
	rpc_sending_chan := make(chan bool, f.io_depth)
	rpc_reply_chan := make(chan *rpc.Rpc_call, f.io_depth)

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
			len(chunk_chan), total_rpc_calls.Load()-recieved_rpc_repies.Load(), len(rpc_reply_chan))
	}

	// get rpc replies
	go func() {
		defer all_rpc_replies_received.Done()
		util.Debugf("NFS File ReadFrom: start liesting for rpc replies.")

		for rpccall := range rpc_reply_chan {
			util.Debugf("NFS File ReadFrom: RPC Reply")

			<-rpc_sending_chan
			show_chan_depth()

			// read NFS reply status
			if _, err := f.NfsReadResponse(rpccall.Res, rpccall.Error); err != nil {
				util.Debugf("NFS File ReadFrom: NfsReadResponse: %s", err.Error())
			}

			writeres := &WriteRes{}

			if err := xdr.Read(rpccall.Res, writeres); err != nil {
				util.Errorf("write(%x) failed to parse result: %s", f.fh, err.Error())
				util.Debugf("write(%x) partial result: %+v", f.fh, writeres)
				break
			}

			if writeres.Count != rpccall.Msg.Body.(*WriteArgs).Count {
				util.Debugf("write(%x) did not write full data payload: sent: %d, written: %d", f.fh, f.fsinfo.WTPref, writeres.Count)
			}

			f.curr += uint64(writeres.Count)
			written_confirmed += writeres.Count

			util.Debugf("write(%x) len=%d new_offset=%d written=%d total=%d", f.fh, f.fsinfo.WTPref, f.curr, writeres.Count, written_confirmed)

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

				rpccall := f.Go(&WriteArgs{
					Header: rpc.Header{
						Rpcvers: 2,
						Prog:    Nfs3Prog,
						Vers:    Nfs3Vers,
						Proc:    NFSProc3Write,
						Cred:    f.auth,
						Verf:    rpc.AuthNull,
					},
					FH:       f.fh,
					Offset:   f.curr,
					Count:    writeSize,
					How:      2,
					Contents: chunk[written : written+writeSize],
				}, rpc_reply_chan)

				if rpccall.Error != nil {
					util.Errorf("write(%x): %s", f.fh, rpccall.Error.Error())
					break
				}

				f.curr += uint64(writeSize)
				written += uint32(writeSize)
			}
		}
		util.Debugf("NFS File ReadFrom: done sending RPCs")
	}()

	// Read the input stream until EOF
	go func() {
		util.Debugf("NFS File ReadFrom: starting to read from input stream")
		chunks_read := 0
		for {
			//todo: a pool of buffers again?
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

// Since we don't know the size of the file, we need to read it in chunks so we don't block
func (f *File) Read(p []byte) (int, error) {
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

	readSize := min(f.fsinfo.RTPref, uint32(len(p)))
	util.Debugf("read(%x) len=%d offset=%d", f.fh, readSize, f.curr)

	r, err := f.call(&ReadArgs{
		Header: rpc.Header{
			Rpcvers: 2,
			Prog:    Nfs3Prog,
			Vers:    Nfs3Vers,
			Proc:    NFSProc3Read,
			Cred:    f.auth,
			Verf:    rpc.AuthNull,
		},
		FH:     f.fh,
		Offset: uint64(f.curr),
		Count:  readSize,
	})

	if err != nil {
		util.Debugf("read(%x): %s", f.fh, err.Error())
		return 0, err
	}

	readres := &ReadRes{}
	if err = xdr.Read(r, readres); err != nil {
		return 0, err
	}

	f.curr = f.curr + uint64(readres.Data.Length)
	n, err := r.Read(p[:readres.Data.Length])
	if err != nil {
		return n, err
	}

	if readres.EOF != 0 {
		err = io.EOF
	}

	return n, err
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
