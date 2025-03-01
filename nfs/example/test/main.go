// Copyright © 2017 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2-Clause
package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/sile16/go-nfs-client/nfs"
	"github.com/sile16/go-nfs-client/nfs/metrics"
	"github.com/sile16/go-nfs-client/nfs/rpc"
	"github.com/sile16/go-nfs-client/nfs/util"
)

func main() {
	util.SetLevelDebug()
	metrics.Metrics_init("go-nfs-client-main-test", 100 )


	if len(os.Args) != 3 {
		util.Errorf("%s <host>:<target path> <test directory to be created>", os.Args[0])
		os.Exit(-1)
	}

	b := strings.Split(os.Args[1], ":")

	host := b[0]
	target := b[1]
	dir := os.Args[2]

	util.Infof("host=%s target=%s dir=%s\n", host, target, dir)

	mount, err := nfs.DialMount(host, false)
	if err != nil {
		log.Fatalf("unable to dial MOUNT service: %v", err)
	}
	defer mount.Close()

	auth := rpc.NewAuthUnix("hasselhoff", 1001, 1001)

	v, err := mount.Mount(target, auth.Auth(), false)
	if err != nil {
		log.Fatalf("unable to mount volume: %v", err)
	}
	defer v.Close()

	// discover any system files such as lost+found or .snapshot
	dirs, err := ls(v, ".")
	if err != nil {
		log.Fatalf("ls: %s", err.Error())
	}
	baseDirCount := len(dirs)

	
	if _, err = v.Mkdir(dir, 0775); err != nil {
		log.Fatalf("mkdir error: %v", err)	
	}

	if _, err = v.Mkdir(dir, 0775); err == nil {
		log.Fatalf("mkdir expected error")
	}

	// make a nested dir
	if _, err = v.Mkdir(dir+"/a", 0775); err != nil {
		log.Fatalf("mkdir error: %v", err)
	}

	// make a nested dir
	if _, err = v.Mkdir(dir+"/a/b", 0775); err != nil {
		log.Fatalf("mkdir error: %v", err)
	}

	dirs, err = ls(v, ".")
	if err != nil {
		log.Fatalf("ls: %s", err.Error())
	}

	// check the length.  There should only be 1 entry in the target (aside from . and .., et al)
	if len(dirs) != 1+baseDirCount {
		log.Fatalf("expected %d dirs, got %d", 1+baseDirCount, len(dirs))
	}

	// 7b file
	if err = testFileRW(v, "7b", 7); err != nil {
		log.Fatalf("fail")
	}


	// 10 MB file
	if err = testFileRW(v, "10mb", 1000*1024*1024); err != nil {
		log.Fatalf("fail")
	}

	
	// should return an error
	if err = v.RemoveAll("7b"); err == nil {
		log.Fatalf("expected a NOTADIR error")
	} else {
		nfserr := err.(*nfs.Error)
		if nfserr.ErrorNum != nfs.NFS3ErrNotDir {
			log.Fatalf("Wrong error")
		}
	}

	if err = v.Remove("7b"); err != nil {
		log.Fatalf("rm(7b) err: %s", err.Error())
	}

	if err = v.Remove("10mb"); err != nil {
		log.Fatalf("rm(10mb) err: %s", err.Error())
	}

	_, _, err = v.Lookup(dir)
	if err != nil {
		log.Fatalf("lookup error: %s", err.Error())
	}

	if _, err = ls(v, "."); err != nil {
		log.Fatalf("ls: %s", err.Error())
	}

	if err = v.RmDir(dir); err == nil {
		log.Fatalf("expected not empty error")
	}

	for _, fname := range []string{"/one", "/two", "/a/one", "/a/two", "/a/b/one", "/a/b/two"} {
		if err = testFileRW(v, dir+fname, 10); err != nil {
			log.Fatalf("fail")
		}
	}

	if err = v.RemoveAll(dir); err != nil {
		log.Fatalf("error removing files: %s", err.Error())
	}

	outDirs, err := ls(v, ".")
	if err != nil {
		log.Fatalf("ls: %s", err.Error())
	}

	if len(outDirs) != baseDirCount {
		log.Fatalf("directory should be empty of our created files!")
	}

	if err = mount.Unmount(); err != nil {
		log.Fatalf("unable to umount target: %v", err)
	}

	mount.Close()
	util.Infof("Completed tests")
}

func testFileRW(v *nfs.Target, name string, filesize uint64) error {

	// create a temp file
	f, err := os.Open("/dev/urandom")
	if err != nil {
		util.Errorf("error openning random: %s", err.Error())
		return err
	}

	wr, err := v.OpenFile(name, 0777)
	if err != nil {
		util.Errorf("write fail: %s", err.Error())
		return err
	}
	wr.SetMaxWriteSize(512*1024)

	//make a buffer and read urandom into it
	//buf := make([]byte, filesize)
	//f.Read(buf)

	//f.Close()

	//new buffer readder from the file

	r := io.LimitReader(f, int64(filesize))

	// calculate the sha
	h := sha256.New()
	t := io.TeeReader(r, h)

	// Copy filesize
	n, err := io.CopyN(wr, t, int64(filesize))
	if err != nil {
		util.Errorf("error copying: n=%d, %s", n, err.Error())
		return err
	}
	expectedSum := h.Sum(nil)

	if err = wr.Close(); err != nil {
		util.Errorf("error committing: %s", err.Error())
		return err
	}

	//
	// get the file we wrote and calc the sum
	rdr, err := v.Open(name)
	if err != nil {
		util.Errorf("read error: %v", err)
		return err
	}

	h = sha256.New()
	t = io.TeeReader(rdr, h)

	// create new buffer to read into from the file
	buf_read := make([]byte, filesize)
	//create writer to write to the buffer
	w := bytes.NewBuffer(buf_read)
	w.Reset()
	t2 := io.TeeReader(t, w)

	a, err := io.ReadAll(t2)
	if len(a) != int(filesize) {
		util.Errorf("readall error: %v", err)
	}
	
	if err != nil {
		util.Errorf("readall error: %v", err)
		return err
	}
	actualSum := h.Sum(nil)

	err = rdr.Close()
	if err != nil {
		util.Errorf("closing rdr error: %v", err)
		return err
	}
	

	/*if !bytes.Equal(buf, buf_read) {
		log.Fatalf("read data didn't match.") 
	}*/

	if !bytes.Equal(actualSum, expectedSum) {
		log.Fatalf("ReadAll sums didn't match. actual=%x expected=%x", actualSum, expectedSum) //  Got=0%x expected=0%x", string(buf), testdata)
	}

	log.Printf("Sums match %x %x", actualSum, expectedSum)

	//check the WriteAt function
	// get the file we wrote and calc the sum
	rdr, err = v.Open(name)
	if err != nil {
		util.Errorf("read error: %v", err)
		return err
	}
	rdr.SetMaxReadSize(64*1024)
	rdr.SetIODepth(4)

	h = sha256.New()
	rdr.WriteTo(h)
	actualSum = h.Sum(nil)
	if !bytes.Equal(actualSum, expectedSum) {
		log.Fatalf("WriteTo sums didn't match. actual=%x expected=%x", actualSum, expectedSum) //  Got=0%x expected=0%x", string(buf), testdata)
	}

	rdr, err = v.Open(name)
	if err != nil {
		util.Errorf("read error: %v", err)
		return err
	}
	rdr.SetMaxReadSize(512*1024)
	rdr.SetIODepth(2)
	rdr.WriteTo(h)

	return nil
}

func ls(v *nfs.Target, path string) ([]*nfs.EntryPlus, error) {
	dirs, err := v.ReadDirPlus(path)
	if err != nil {
		return nil, fmt.Errorf("readdir error: %s", err.Error())
	}

	util.Infof("dirs:")
	for _, dir := range dirs {
		util.Infof("\t%s\t%d:%d\t0%o", dir.FileName, dir.Attr.Attr.UID, dir.Attr.Attr.GID, dir.Attr.Attr.Mode)
	}

	return dirs, nil
}
