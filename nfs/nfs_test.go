// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
package nfs

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/sile16/go-nfs-client/nfs/rpc"

)

func listenAndServe(t *testing.T, port int) (*net.TCPListener, *sync.WaitGroup, error) {

	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		l.Accept()
		t.Logf("Accepted conn")
		l.Accept()
		t.Logf("Accepted conn")
		wg.Done()
	}()

	return l, wg, nil
}
 
/*
func panicOnErr(t *testing.T, err error, desc ...interface{}) {
	if err == nil {
		return
	}
	t.Log(desc...)
	t.Log(err.Error())
}*/

// test we can bind without colliding
func TestDialService(t *testing.T) {
	listener, wg, err := listenAndServe(t, 6666)
	if err != nil {
		t.Logf("error starting listener: %s", err.Error())
		t.Fail()
		return
	}
	defer listener.Close()

	_, err = dialService("127.0.0.1", 6666, false)
	if err != nil {
		t.Logf("error dialing: %s", err.Error())
		t.FailNow()
	}

	_, err = dialService("127.0.0.1", 6666, false)
	if err != nil {
		t.Logf("error dialing: %s", err.Error())
		t.FailNow()
	}

	wg.Wait()
}


func BenchmarkWriteFile(b *testing.B) {

	//metrics.DefaultConfig.CollectionInterval = time.Second
	/*
    if err := metrics.RunCollector(metrics.DefaultConfig); err != nil {
        b.Logf("error starting metrics collector: %s", err.Error())
		b.FailNow()
    }*/

	buf := make([]byte, 8 * 1024 * 1024)

	// rand.Read(buf)

	//b.Log("First Mount")
	mount_dst, err := DialMount("192.168.20.20", true)
	if err != nil {
		b.Logf("error dialing mount: %s", err.Error())
		b.FailNow()
	}
	defer mount_dst.Close()

	hostname := "testhost"
	user_id := os.Getuid()
	group_id := os.Getgid()

	auth := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))

	//b.Log("create target")
	target_dst, err := mount_dst.Mount("/data", auth.Auth(), true)
	if err != nil {
		b.Logf("error mounting: %s", err.Error())
		b.FailNow()
	}
	defer target_dst.Close()

	io_depth := []int {1, 4, 8 }
	io_size := []uint32 {4, 32, 256, 512, 1024}

    for _, io_s := range io_size {
		for _, io_d := range io_depth {
			// t.Run enables running "subtests", one for each
			// table entry. These are shown separately
			// when executing `go test -v`.
			testname := fmt.Sprintf("BenchmarkWriteFile_%dK_RpcDepth_%d", io_s, io_d)
			
			b.Run(testname, func(b *testing.B) {
				r := bytes.NewReader(buf)
				//for n := 0; n < b.N; n++ {
					//b.Log("create file")
					f_dst, err := target_dst.OpenFile("golang_nfs_test", os.FileMode(int(0644)))
					
					if err != nil {
						b.Logf("error opening file: %s", err.Error())
						b.FailNow()
					}

					f_dst.SetMaxWriteSize(io_s * 1024)
					f_dst.SetIODepth(io_d)
					
					//b.Log("running readfrom.")
					f_dst.ReadFrom(r)
					//b.Log("closing file")
					f_dst.Close()
				//}
			})
		}
	} 
}
