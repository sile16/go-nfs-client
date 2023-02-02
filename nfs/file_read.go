package nfs

import (
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

//make a sync pool of channels of type *rpc.Rpc_call
var rpc_reply_chan_pool = sync.Pool{
	New: func() interface{} {
		return make(chan *rpc.Rpc_call, 1)
	},
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
	rpccall.ReturnedMsg = readres

	if rpccall.Error != nil {
		return readres, rpccall.Error
	}

	var n int

	if p != nil {
		rpccall.ReturnedData = p
	} else {
		rpccall.ReturnedData = make([]byte, readres.Data.Length)
	}
	

	n, rpccall.Error = rpccall.Res.Read(rpccall.ReturnedData[:readres.Data.Length])

	if n != int(readres.Data.Length) {
		rpccall.Error = io.ErrShortBuffer
	}

	if readres.EOF != 0 {
		rpccall.Error = io.EOF
	}

	metrics.MC["RpcBytesReadCounter"].Add(float64(readres.Data.Length))
	metrics.MH["RpcIOSizeReceive"].Observe(float64(readres.Data.Length))
	return readres, rpccall.Error
}

// send read rpc call, params offset, count and read_done channel
func (f *File) send_read_rpc(count uint32, offset int, read_done chan *rpc.Rpc_call) *rpc.Rpc_call {
	metrics.MC["RpcReadRequestsCounter"].Inc()
	metrics.MH["RpcIOSizeRequest"].Observe(float64(count))
	

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

		//make a list of rpc calls needed to buffer for missing read data
		
		waiting_for_missing_data := make([]*rpc.Rpc_call, 0, f.io_depth)
		next_offset := 0
		eof := false
		largest_response := uint32(0)
		too_small_response_count := uint32(0)

		for {
			
			select {
			case <-done_chan:
				return
			case rpc_res := <-rpc_reply_chan:
				
				
				readres, err := f.process_read_response(rpc_res, nil)
				if err != nil {
					if err == io.EOF {
						eof = true
					} else {
						write_to_error = err
						return
					}
				}
					

				
				if readres.Data.Length > largest_response {
					largest_response = readres.Data.Length
				}

				// if we have missing data, we need send a request for the missing data
				// and buffer the response
				if rpc_res.Msg.Body.(*ReadArgs).Count != readres.Data.Length {
					read_size := rpc_res.Msg.Body.(*ReadArgs).Count - readres.Data.Length
					offset := rpc_res.Msg.Body.(*ReadArgs).Offset + uint64(readres.Data.Length)
					f.send_read_rpc(read_size, int(offset), rpc_reply_chan)
					too_small_response_count++

					if too_small_response_count > 4 {
						util.Debugf("too many small responses, reducing request to size: %d", largest_response)
						f.SetMaxReadSize(largest_response)
						too_small_response_count = 0
						largest_response = 0
					}
				}

				// insert message into our queue
				inserted := false
				for i, rpc_tmp := range waiting_for_missing_data {
					if rpc_tmp == nil {
						waiting_for_missing_data[i] = rpc_res
						inserted=true
						break
					}
				}
				if !inserted {
					waiting_for_missing_data = append(waiting_for_missing_data, rpc_res)
				}
				
				// search queue to see if we have the next bytes in the stream
				all_nil := true
				for found := true; found; {
					found = false
					all_nil = true
					for i, rpc_tmp := range waiting_for_missing_data {
						if rpc_tmp != nil {
							all_nil = false
							offset := rpc_tmp.Msg.Body.(*ReadArgs).Offset
							if offset == uint64(next_offset) {
								found = true
								len := rpc_tmp.ReturnedMsg.(*ReadRes).Data.Length
								p := rpc_tmp.ReturnedData
								// we have the missing data, so we can write it out
								<- max_outstanding_rpc_chan	
								data_chan <- p[:len]
								next_offset += int(len)
								
								// remove the rpc_res from the waiting list
								waiting_for_missing_data[i] = nil
							} else if offset < uint64(next_offset) {
								util.Debugf("unexpected offset %d, expected %d", offset, next_offset)
							}
						}
					}
				}
				if eof && all_nil {
					return
				}
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
			max_outstanding_rpc_chan <- 1  // this will rate limit the outstanding rpcs
			
			f.send_read_rpc(f.max_read_size, int(f.curr), rpc_reply_chan)
			
			f.curr += uint64(f.max_read_size)

			if f.curr >= uint64(f.size) {
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

	//create a channel sync.pool of type chan *Rpc.rpc_call for rpc_replies
	
	rpc_reply_chan := rpc_reply_chan_pool.Get().(chan *rpc.Rpc_call)
	defer rpc_reply_chan_pool.Put(rpc_reply_chan)

	var mux sync.Mutex
	mux.Lock()
	rpc_call := f.send_read_rpc(uint32(len(p)), int(f.curr), rpc_reply_chan)
	//mux.Unlock()
	
	rpc_res := <- rpc_reply_chan

	//mux.Lock()
	read_res, err := f.process_read_response(rpc_res, p)

	f.curr += uint64(read_res.Count)
	mux.Unlock()
	//print the current offset and length of the read
	util.Debugf("read req_offset=%11d, req_len=%11d returned_len=%11d EOF=%2d", 
					rpc_call.Msg.Body.(*ReadArgs).Offset, 
					rpc_call.Msg.Body.(*ReadArgs).Count,
					read_res.Data.Length,
					read_res.EOF)

	return int(read_res.Count), err
}

