package surfstore

import (
	context "context"
	"errors"
	"sync"
)

var blockMutex sync.Mutex

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	blockMutex.Lock()
	defer blockMutex.Unlock()
	block, ok := bs.BlockMap[blockHash.GetHash()]
	if !ok {
		return nil, errors.New("surfStore:GetBlock: don't have such block")
	}
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	blockMutex.Lock()
	defer blockMutex.Unlock()
	hashName := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hashName] = block
	success := Success{Flag: true}
	// fmt.Println(bs.BlockMap)
	return &success, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockMutex.Lock()
	defer blockMutex.Unlock()
	hashes := blockHashesIn.GetHashes()
	resp := &BlockHashes{}
	for _, hash := range hashes {
		_, ok := bs.BlockMap[hash]
		if ok {
			resp.Hashes = append(resp.Hashes, hash)
		}
	}
	return resp, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
