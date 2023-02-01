package nfs

import (
	"bytes"
	"io"
	"sync"

	"github.com/sile16/go-nfs-client/nfs/metrics"
	"github.com/sile16/go-nfs-client/nfs/rpc"
	"github.com/sile16/go-nfs-client/nfs/util"
	"github.com/sile16/go-nfs-client/nfs/xdr"
)

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

// proces read response from rpc call
func (f *File) process_read_response(rpccall *rpc.Rpc_call, p []byte) (*ReadRes, error) {

	// read NFS reply status
	if _, err := f.NfsReadResponse(rpccall.Res, rpccall.Error); err != nil {
		util.Debugf("NFS File ReadFrom: NfsReadResponse: %s", err.Error())
		rpccall.Error = err
		return nil, err
	}

	readres := &ReadRes{}

	rpccall.Error = xdr.Read(rpccall.Res, readres)

	if rpccall.Error != nil {
		return readres, rpccall.Error
	}

	var n int

	n, rpccall.Error = rpccall.Res.Read(p[:readres.Data.Length])
	if n != int(readres.Data.Length) {
		rpccall.Error = io.ErrShortBuffer
	}

	if readres.EOF != 0 {
		rpccall.Error = io.EOF
	}

	return readres, rpccall.Error
}

// send read rpc call, params offset, count and read_done channel
func (f *File) send_read_rpc(count uint32, offset int, read_done chan *rpc.Rpc_call) *rpc.Rpc_call {
	metrics.RpcReadRequestsCounter.Inc()
	metrics.RpcBytesReadCounter.Add(float64(count))

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

func (f *File) WriteTo(w io.Writer) (n int64, err error) {
	util.Debugf("read file: %s at offset=%d",f.fsinfo.Attr.Attr.Name(), f.curr)

	//start a go routine to process the read rpcs responses
	rpc_reply_chan := make(chan *rpc.Rpc_call, f.io_depth)
	max_outstanding_rpc_chan := make(chan int, f.io_depth)
	done_chan := make(chan bool, 1)
	bytes_written := int64(0)
	write_to_error := error(nil)
	//a waitgroup
	wg := sync.WaitGroup{}
	wg.Add(3)
	

	//channel to pass the data buffers to the write go routine
	data_chan := make(chan []byte, f.io_depth)

	// start a go routine with a buffer channel to write to the writer w
	go func() {
		defer wg.Done()

		for data := range data_chan  { // we block here.
			
			n, err := w.Write(data)
			bytes_written += int64(n)
			
			if n != len(data) {
				write_to_error = io.ErrShortWrite
				break
			}
			if err != nil {
				write_to_error = err
				break
			}
			//if bytes_written >= int64(f.size) {
			// we should read until we get an EOF
			//	break
			//}
		}
		close(done_chan)
	}()

	go func() {
		defer wg.Done()
		defer close(data_chan)
		for {
			// buffer to use for the read
			select {
			case <-done_chan:
				return
			case rpc_res := <-rpc_reply_chan:

				p := make([]byte, f.max_read_size)
				readres, err := f.process_read_response(rpc_res, p)

				data_chan <- p[:readres.Count]	

				if err != nil {
					write_to_error = err
					return
				}
				<- max_outstanding_rpc_chan	
			}
		}
		
	}()

	// start a go routine to send the rpcs
	go func() {
		defer wg.Done()
		defer close(max_outstanding_rpc_chan)
		for {
			// send the read rpc
			//size := min(f.max_read_size, uint32(f.size)-uint32(f.curr))
			
			f.send_read_rpc(f.max_read_size, int(f.curr), rpc_reply_chan)
			max_outstanding_rpc_chan <- 1  // this will rate limit the outstanding rpcs
			f.curr = f.curr + uint64(f.max_read_size)

			if f.curr >= uint64(f.max_read_size) {
				break
			}
		}
	}()

	// waitgroup
	wg.Wait()

	return bytes_written, write_to_error
}

// Since we don't know the size of the file, we need to read it in chunks so we don't block
func (f *File) Read(p []byte) (int, error) {

	//todo: have to read until end of file or size of p
	//read_response_chan := make(chan *rpc.Rpc_call, 4)

	//rpccall_chan := f.ReadAsync(p, int(f.curr), read_response_chan).DoneChan
	//rpccall := <-rpccall_chan
	//return len(rpccall.Res_bytes), rpccall.Error
	var mux sync.Mutex
	mux.Lock()
	buffer := bytes.NewBuffer(p)
	buffer.Reset()
	n, err := f.WriteTo(buffer)
	mux.Unlock()
	

	return int(n), err
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
