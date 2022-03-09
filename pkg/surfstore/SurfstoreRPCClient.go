package surfstore

import (
	context "context"
	"errors"
	"os"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	grpc "google.golang.org/grpc"
)

type RPCClient struct {
	MetaStoreAddr []string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	s := resp.GetFlag()
	succ = &s
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.GetHashes()
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for i := 0; i < len(surfClient.MetaStoreAddr); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddr[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if err.Error() == "rpc error: code = Unknown desc = Server is not the leader" {
				continue
			} else if err.Error() == "rpc error: code = Unknown desc = Server is crashed." {
				continue
			} else if err == context.DeadlineExceeded {
				os.Exit(1)
			} else {
				conn.Close()
				return err
			}
		}
		*serverFileInfoMap = b.GetFileInfoMap()
		// close the connection
		return conn.Close()
	}
	return errors.New("GetFileInfoMap: call failed")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for i := 0; i < len(surfClient.MetaStoreAddr); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddr[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			if err.Error() == "rpc error: code = Unknown desc = Server is not the leader" {
				continue
			} else if err.Error() == "rpc error: code = Unknown desc = Server is crashed." {
				continue
			} else if err == context.DeadlineExceeded {
				os.Exit(1)
			} else {
				conn.Close()
				return err
			}
		}
		*latestVersion = b.GetVersion()
		// close the connection
		return conn.Close()
	}
	return errors.New("UpdateFile: fall failed")
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for i := 0; i < len(surfClient.MetaStoreAddr); i++ {
		conn, err := grpc.Dial(surfClient.MetaStoreAddr[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if err != nil {
			//fmt.Printf("GetBlockStoreAddr: err=%v\n", err.Error() == "rpc error: code = Unknown desc = Server is crashed.")
			if err.Error() == "rpc error: code = Unknown desc = Server is not the leader" {
				continue
			} else if err.Error() == "rpc error: code = Unknown desc = Server is crashed." {
				continue
			} else if err == context.DeadlineExceeded {
				os.Exit(1)
			} else {
				conn.Close()
				return err
			}
		}
		*blockStoreAddr = b.GetAddr()
		// close the connection
		return conn.Close()
	}
	return errors.New("GetBlockStoreAddr: fall failed")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddr: addrs,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
