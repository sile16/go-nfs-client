package nfs

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"github.com/sile16/go-nfs-client/nfs/metrics"
	"github.com/sile16/go-nfs-client/nfs/rpc"
	"github.com/sile16/go-nfs-client/nfs/util"
	"github.com/sile16/go-nfs-client/nfs/xdr"
)


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

// send write rpc call, params offset, count and read_done channel
func (f *File) send_write_rpc(buf []byte, offset int, write_done_chan chan *rpc.Rpc_call) *rpc.Rpc_call {
	metrics.MC["RpcWriteRequestsCounter"].Inc()
	metrics.MH["RpcIOSizeRequest"].Observe(float64(len(buf)))
	

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
		Offset:   uint64(offset),
		Count:    uint32(len(buf)),
		How:      2,
		Contents: buf,
	}, write_done_chan)

	return rpccall

}

// Process the write response from an rpc call
func (f *File) process_write_response(rpccall *rpc.Rpc_call) *WriteRes {

	// read NFS reply status
	if _, err := f.NfsReadResponse(rpccall.Res, rpccall.Error); err != nil {
		util.Debugf("NFS File ReadFrom: NfsReadResponse: %s", err.Error())
		rpccall.Error = err
	}

	writeres := &WriteRes{}

	if err := xdr.Read(rpccall.Res, writeres); err != nil {
		util.Errorf("write(%x) failed to parse result: %s", f.fh, err.Error())
		util.Debugf("write(%x) partial result: %+v", f.fh, writeres)
	}

	if writeres.Count != rpccall.Msg.Body.(*WriteArgs).Count {
		util.Debugf("write(%x) did not write full data payload: sent: %d, written: %d", f.fh, f.fsinfo.WTPref, writeres.Count)
	}
	metrics.MC["RpcBytesWrittenCounter"].Add(float64(writeres.Count))
	metrics.MH["RpcIOSizeReceive"].Observe(float64(writeres.Count))

	return writeres

}

// implements the file ReadFrom, will readfrom reader and write to the open file.
func (f *File) ReadFrom(r io.Reader) (int64, error) {

	chunk_chan := make(chan []byte, f.io_depth)
	rpc_sending_chan := make(chan bool, f.io_depth)
	rpc_reply_chan := make(chan *rpc.Rpc_call, f.io_depth)

	util.Debugf("ReadFrom channels created with depth %d", f.io_depth)

	var recieved_rpc_repies atomic.Int32
	var total_rpc_calls atomic.Int32
	
	var all_sent atomic.Bool  

	wg := sync.WaitGroup{}
	wg.Add(3)

	// Read rpc replies
	var written_confirmed atomic.Uint64
	var written_sent atomic.Uint64

	// get rpc write replies
	go func() {
		defer wg.Done()
		
		for rpccall := range rpc_reply_chan {
			util.Debugf("NFS File ReadFrom: RPC Reply")

			// this pulls item out of the channel to indicate another request can be sent
			<-rpc_sending_chan

			// process the write response
			writeres := f.process_write_response(rpccall)
			written_confirmed.Add(uint64(writeres.Count))
			
			recieved_rpc_repies.Add(1)

			if all_sent.Load() && (total_rpc_calls.Load() <= recieved_rpc_repies.Load()) {
				break
			}

			if all_sent.Load() && written_confirmed.Load() >= written_sent.Load() {
				break
			}
		}
		util.Debugf("NFS File ReadFrom: done processing rpc replies.")
	}()

	// Send RPC requests
	
	go func() {
		defer all_sent.Store(true)
		defer wg.Done()

		for chunk := range chunk_chan {
			totalToWrite := uint64(len(chunk))

			for written := uint64(0); written < totalToWrite; {
				rpc_sending_chan <- true // will rate limit outstanding RPCs based on buff depth
				writeSize := min(uint64(f.max_write_size), totalToWrite-written)
				
				//use the async write call to send the call
				f.send_write_rpc(chunk[written:written+writeSize], int(f.curr), rpc_reply_chan)
				written_sent.Add(writeSize)
				total_rpc_calls.Add(1)

				f.curr += uint64(writeSize)
				written += writeSize
			}
		}
		util.Debugf("NFS File ReadFrom: done sending RPCs")
	}()

	// Read the input stream until EOF
	// on the Read size this is currently only single out standing request unless it's a buffered reader.
	go func() {
		//todo: increase the read chunk size ?
		defer wg.Done()
		defer close(chunk_chan)

		for {
			//todo: a pool of buffers again?
			// We the buffer size to the max write size of the NFS server.

			buf := make([]byte, f.max_write_size)
			n, err := r.Read(buf)
			chunk_chan <- buf[:n]

			if err != nil {
				if err == io.EOF {
					break
				}
				util.Errorf("NFS File ReadFrom: read error: %s", err.Error())
				break
			}
		}
		util.Debugf("NFS File ReadFrom: done reading from source buffer")
	}()

	wg.Wait()
	return int64(written_confirmed.Load()), nil
}

// Write entire buffer to file, will loop until all data is written
// will issue multiple writes up to the depth in specified DialMount Args
func (f *File) Write(p []byte) (int, error) {
	n, err := f.ReadFrom(bytes.NewReader(p))
	return int(n), err
}

