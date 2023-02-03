package nfs

import (
	"errors"
	"io"
	"os"
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
	curr      uint64
	size      int64
	fsinfo    *FSInfo
	io_depth  int
	max_write_size uint32
	max_read_size uint32

	// filehandle to the file
	fh []byte
}

// sets the number of concurrent io operations for sync functions readfrom, & write.
func (f *File) SetIODepth(depth int) {
	f.io_depth = depth
}

// sets the max write size for sync functions readfrom, & write.
func (f *File) SetMaxWriteSize(size uint32) {
	atomic.StoreUint32(&f.max_write_size, size)
}

// sets the max read size for sync functions readfrom, & write.
func (f *File) SetMaxReadSize(size uint32) {
	//lock with mutex
	atomic.StoreUint32(&f.max_read_size, size)
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



// func (c *Client) Call(call interface{}) (io.ReadSeeker, error) {
//	rpccall_chan := c.Go(call, make(chan *Rpc_call, 1)).Done
//	rpccall := <-rpccall_chan
//	return rpccall.Res, rpccall.Error
//}



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
		max_read_size: v.fsinfo.RTPref,
		max_write_size: v.fsinfo.WTPref,
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
		max_read_size: v.fsinfo.RTPref,
		max_write_size: v.fsinfo.WTPref,
		fh:     fh,
		io_depth: 2, //default in linux rpc
	}

	return f, nil
}

func min(x, y uint64) uint64 {
	if x > y {
		return y
	}
	return x
}

