// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
package nfs

import (
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
	curr   uint64
	size   int64
	fsinfo *FSInfo

	// filehandle to the file
	fh []byte
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

	chunk_chan := make(chan []byte, 50)
	rpc_chan := make(chan *rpc.Rpc_call, 8)
	//rpc_reply := make(chan bool, 8)
	//done_chan := make(chan bool, 1)
	

	var total_rpc_calls atomic.Int32
	var recieved_rpc_repies atomic.Int32
	var all_sent atomic.Bool
	
	// Read rpc replies
	written_confirmed := uint32(0)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		
		for rpccall := range rpc_chan {

				writeres := &WriteRes{}
				
				if err := xdr.Read(rpccall.Res, writeres); err != nil {
					util.Errorf("write(%x) failed to parse result: %s", f.fh, err.Error())
					util.Debugf("write(%x) partial result: %+v", f.fh, writeres)
					break
				}

				if writeres.Count != f.fsinfo.WTPref {
					util.Debugf("write(%x) did not write full data payload: sent: %d, written: %d", f.fsinfo.WTPref, writeres.Count)
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
		
		for chunk := range chunk_chan {
			total_rpc_calls.Add(1)
			
			writeSize := uint32(len(chunk))
			
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
				Contents: chunk,
			}, rpc_chan)

			if rpccall.Error != nil {
				util.Errorf("write(%x): %s", f.fh, rpccall.Error.Error())
				break
			}

			f.curr += uint64(writeSize)
			//written += uint64(writeSize)
		}
	}()

	// Read the input stream until EOF
	go func() {	
		for {
			//todo: a pool of buffers again?
			buf := make([]byte, f.fsinfo.WTPref)
			n, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				}
				
			}
			chunk_chan <- buf[:n]
		}
		close(chunk_chan)
		
	}()

	wg.Wait()
	return int64(written_confirmed), nil
}

//Since we don't know the size of the file, we need to read it in chunks so we don't block
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

	totalToWrite := uint32(len(p))
	written := uint32(0)



	for written = 0; written < totalToWrite; {
		writeSize := min(f.fsinfo.WTPref, totalToWrite-written)

		res, err := f.call(&WriteArgs{
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
			Contents: p[written : written+writeSize],
		})

		if err != nil {
			util.Errorf("write(%x): %s", f.fh, err.Error())
			return int(written), err
		}

		writeres := &WriteRes{}
		if err = xdr.Read(res, writeres); err != nil {
			util.Errorf("write(%x) failed to parse result: %s", f.fh, err.Error())
			util.Debugf("write(%x) partial result: %+v", f.fh, writeres)
			return int(written), err
		}

		if writeres.Count != writeSize {
			util.Debugf("write(%x) did not write full data payload: sent: %d, written: %d", writeSize, writeres.Count)
		}

		f.curr += uint64(writeres.Count)
		written += writeres.Count

		util.Debugf("write(%x) len=%d new_offset=%d written=%d total=%d", f.fh, totalToWrite, f.curr, writeres.Count, written)
	}

	return int(written), nil
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
		Target: v,
		fsinfo: v.fsinfo,
		fh:     fh,
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

