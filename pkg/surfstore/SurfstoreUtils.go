package surfstore

import (
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// compare and update phase
	blockSize := client.BlockSize
	baseDir := client.BaseDir
	blockStoreAddr := ""
	blockStoreAddrPtr := &blockStoreAddr
	// get block store address
	err := client.GetBlockStoreAddr(blockStoreAddrPtr)
	if err != nil {
		log.Printf("ClientSync: Err when getting block server address, err=%v\n", err)
		return
	}
	// fmt.Printf("[ClientSync] block addr=%v\n", *blockStoreAddrPtr)
	// load local file information map
	localFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Println("Error occurred when reading index.txt")
		return
	}
	// scan base directory
	dir, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Printf("ClientSync: Err when scanning base dir, err=%v\n", err)
		return
	}
	// allFile: a set of all files' names
	allFiles := make(map[string]bool)
	for _, fileInfo := range dir {
		if fileInfo.IsDir() || fileInfo.Name() == "index.txt" {
			continue
		}
		allFiles[fileInfo.Name()] = true
	}
	// find DELETED file
	for fileName, fileInfo := range localFileInfoMap {
		if len(fileInfo.GetBlockHashList()) == 1 && fileInfo.GetBlockHashList()[0] == "0" {
			// if map shows deleted, just skip
			continue
		}
		_, ok := allFiles[fileName]
		if !ok {
			// found deleted file
			respVersion, err := uploadFile(fileName, fileInfo.GetVersion()+1, client, blockStoreAddr, []string{"0"})
			if err != nil {
				log.Printf("ClientSync: Err when update deleted file info in meta store, err=%v\n", err)
				return
			}
			if respVersion == fileInfo.GetVersion()+1 {
				// delete success
				localFileInfoMap[fileName].Version = respVersion
				localFileInfoMap[fileName].BlockHashList = []string{"0"}
			}
		}
	}
	// find NEW file/ find MODIFIED file/ find REVIVED file
	for _, fileInfo := range dir {
		if fileInfo.IsDir() || fileInfo.Name() == "index.txt" {
			continue
		}
		hashList, err := computeFileHashList(ConcatPath(baseDir, fileInfo.Name()), blockSize)
		if err != nil {
			log.Printf("ClientSync: Err when update computing blocks' hash, err=%v\n", err)
			return
		}
		localFileInfo, ok := localFileInfoMap[fileInfo.Name()]
		if !ok {
			// find brand-new file
			respVersion, err := uploadFile(fileInfo.Name(), 1, client, blockStoreAddr, hashList)
			if err != nil {
				log.Printf("ClientSync: Err when upload brand-new file, err=%v\n", err)
				return
			}
			if respVersion == 1 {
				// upload brand-new success
				localFileInfoMap[fileInfo.Name()] = &FileMetaData{Filename: fileInfo.Name(), Version: respVersion, BlockHashList: hashList}
			}
		} else if !checkHashList(hashList, localFileInfo.GetBlockHashList()) {
			// find modified file/ revived file
			log.Println("Found revived/modified file " + fileInfo.Name())
			respVersion, err := uploadFile(fileInfo.Name(), localFileInfo.GetVersion()+1, client, blockStoreAddr, hashList)
			if err != nil {
				log.Printf("ClientSync: Err when upload modified/revived file, err=%v\n", err)
				return
			}
			log.Printf("upload file finished, version=%v", respVersion)
			if respVersion == localFileInfo.GetVersion()+1 {
				// update modification success
				localFileInfoMap[fileInfo.Name()].Version = localFileInfo.GetVersion() + 1
				localFileInfoMap[fileInfo.Name()].BlockHashList = hashList

			}
		} else {
			// no change
			continue
		}
	}
	// start download phase
	// get remote file info map
	remoteFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteFileInfoMap)
	if err != nil {
		log.Printf("ClientSync: Err when getting romote file info map, err=%v\n", err)
		return
	}
	for remoteFileName, remoteFileInfo := range remoteFileInfoMap {
		localFileInfo, ok := localFileInfoMap[remoteFileName]
		if !ok || remoteFileInfo.GetVersion() > localFileInfo.GetVersion() {
			if len(remoteFileInfo.GetBlockHashList()) == 1 && remoteFileInfo.GetBlockHashList()[0] == "0" {
				// tombstone
				_, err := os.Stat(ConcatPath(baseDir, remoteFileName))
				if err == nil {
					// file existed
					err := os.Remove(ConcatPath(baseDir, remoteFileName))
					if err != nil {
						log.Printf("ClientSync: Err when downloading new remote file, err=%v\n", err)
						return
					}
				}
			} else {
				// start download

				newFileBytes, _, err := downloadFileFromBlockStore(client, remoteFileInfo.GetBlockHashList())
				if err != nil {
					log.Printf("ClientSync: Err when downloading new remote file, err=%v\n", err)
				}
				f, err := os.Create(ConcatPath(baseDir, remoteFileName))
				_, err = f.Write(*newFileBytes)
				if err != nil {
					log.Printf("ClientSync: Err when writing new remote file, err=%v\n", err)
				}
				f.Close()
			}
		}
	}
	err = WriteMetaFile(remoteFileInfoMap, baseDir)
	if err != nil {
		log.Printf("ClientSync: Err when writing back index.txt, err=%v\n", err)
		return
	}
}

func checkHashList(oldHashList []string, newHashList []string) bool {
	if len(oldHashList) != len(newHashList) {
		return false
	}
	for i := 0; i < len(oldHashList); i++ {
		if oldHashList[i] != newHashList[i] {
			return false
		}
	}
	return true
}
func computeFileHashList(filePath string, blockSize int) ([]string, error) {
	hashList := make([]string, 0)
	fileBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	left := 0
	for left < len(fileBytes) {
		right := left + blockSize
		if right > len(fileBytes) {
			right = len(fileBytes)
		}
		hashList = append(hashList, GetBlockHashString(fileBytes[left:right]))
		left = right
	}
	return hashList, nil
}

// uploadFile: now can handle deleted file
func uploadFile(fileName string, version int32, client RPCClient, blockStoreAddr string, hashList []string) (int32, error) {
	deleted := (len(hashList) == 1) && (hashList[0] == "0")
	filePath := ConcatPath(client.BaseDir, fileName)
	var respVersion int32
	var success bool
	successPtr := &success
	respVersionPtr := &respVersion
	blockSize := client.BlockSize
	if !deleted {
		// if not deleted file, put all blocks to block store
		fileBytes, err := ioutil.ReadFile(filePath)
		if err != nil {
			return -1, err
		}
		left := 0
		for left < len(fileBytes) {
			right := left + blockSize
			if right > len(fileBytes) {
				right = len(fileBytes)
			}
			block := &Block{BlockData: fileBytes[left:right], BlockSize: int32(right - left)}
			err := client.PutBlock(block, blockStoreAddr, successPtr)
			if err != nil {
				return -1, err
			}
			left = right
		}
	}
	// update file info in meta store
	fileMetaData := &FileMetaData{Filename: fileName, Version: version, BlockHashList: hashList}
	err := client.UpdateFile(fileMetaData, respVersionPtr)
	if err != nil {
		return -1, err
	}
	return respVersion, nil
}

func downloadFileFromBlockStore(client RPCClient, hashList []string) (*[]byte, int, error) {
	blockStoreAddr := ""
	blockStoreAddrPtr := &blockStoreAddr
	var fileBytes []byte
	fileSize := 0
	err := client.GetBlockStoreAddr(blockStoreAddrPtr)
	//fmt.Printf("block addr=%v\n", *blockStoreAddrPtr)
	if err != nil {
		return nil, 0, err
	}
	for _, blockHash := range hashList {
		block := &Block{}
		err := client.GetBlock(blockHash, blockStoreAddr, block)
		if err != nil {
			return nil, 0, err
		}
		fileBytes = append(fileBytes, block.GetBlockData()...)
		fileSize += int(block.GetBlockSize())
	}
	return &fileBytes, fileSize, nil
}
