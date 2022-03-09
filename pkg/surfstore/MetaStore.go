package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var metaMutex sync.Mutex

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	metaMutex.Lock()
	defer metaMutex.Unlock()
	fileInfoMao := &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return fileInfoMao, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	metaMutex.Lock()
	defer metaMutex.Unlock()
	filename := fileMetaData.GetFilename()
	version := fileMetaData.GetVersion()
	blockHashList := fileMetaData.GetBlockHashList()
	oldFileMetaData, ok := m.FileMetaMap[filename]
	resp := &Version{}
	if !ok {
		newFileMetadata := &FileMetaData{Filename: filename, Version: version, BlockHashList: blockHashList}
		m.FileMetaMap[filename] = newFileMetadata
		resp.Version = version
	} else {
		// version check
		if oldFileMetaData.Version+1 == fileMetaData.GetVersion() {
			m.FileMetaMap[filename].Version = version
			m.FileMetaMap[filename].BlockHashList = blockHashList
			resp.Version = version
		} else {
			resp.Version = -1
		}
	}
	//fmt.Println(fileMetaData.GetFilename(), fileMetaData.GetVersion(), m.FileMetaMap[fileMetaData.GetFilename()].GetVersion())
	return resp, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	resp := &BlockStoreAddr{}
	resp.Addr = m.BlockStoreAddr
	return resp, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
