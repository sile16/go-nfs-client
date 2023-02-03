package nfs

import (
   "os"
   "errors"
   "strings"
   "github.com/sile16/go-nfs-client/nfs/rpc"
   //"github.com/sile16/go-nfs-client/nfs/util"
   //"github.com/sile16/go-nfs-client/nfs/xdr"
)

//NFS Client structure
type Client struct {
	// RPC client
	//rpcClient *rpc.Client
	nfsMount *Mount
	authUnix *rpc.AuthUnix
	target *Target

}


// NewClient creates a new NFS client
func NewNFSClient(addr string, mount_point string) (*Client, error) {


	// Try and mount to verify
	mount, err := DialMount(addr, true)
	if err != nil {
		err := errors.New("[error] NFS Client Unable to dial mount service, ")
		return nil, err
	}

	hostname := getShortHostname()
	user_id := os.Getuid()
	group_id := os.Getgid()

	authUnix := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))

	target, err := mount.Mount(mount_point, authUnix.Auth(), true)
	if err != nil {
		err := errors.New("[error] FlexFile Unable to mount export, ")
		mount.Close()
		return nil, err
	}
	

	return &Client{
		//rpcClient: rpcClient,
		nfsMount: mount,
		authUnix: authUnix,
		target: target,
	}, nil
}

func getShortHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		//fmt.Println("Warning, null hostname.")
		hostname = "null"
	}
	// Use only the short hostname because dots are invalid in filesystem names.
	hostname = strings.Split(hostname, ".")[0]
	return strings.ToLower(hostname)
}

// Open opens a file for reading
func (c *Client) Open(path string) (*File, error) {
	return c.target.Open(path)
}