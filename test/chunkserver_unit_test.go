package main

import (
	//"flag"
	"fmt"
	"log"
	"net/rpc"
	"testing"
	"time"
	"os"

	//"os"
	"strconv"

	"github.com/sutd_gfs_project/chunkserver"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
	"github.com/theritikchoure/logx"
)

// Test Chunk Storage functions
func TestGetChunk(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var chunkRetrieved models.Chunk
	portNumber := 9000
	chunkServerInstance := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	chunkHandle := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")

	for _, val := range chunkServerInstance.Storage {

		if val.ChunkHandle == chunkHandle && val.ChunkIndex == 0 {
			chunkRetrieved = val
			break
		}
	}
	log.Printf("[Chunkserver %d: GetChunk] Chunk Retrieved: %v\n", portNumber, chunkRetrieved)

}

func TestAddChunk(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var chunkAdded models.Chunk
	portNumber := 9000
	chunkServerInstance := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	chunkHandle := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")
	log.Printf("[ChunkServer %d: AddChunk] ============== ADDING CHUNK ==============\n", portNumber)
	log.Printf("[ChunkServer %d: AddChunk] Chunk added: {%v %d %s}\n", chunkServerInstance.PortNum, chunkHandle, 1, helper.TruncateOutput(chunkAdded.Data))
	chunkServerInstance.Storage = append(chunkServerInstance.Storage, chunkAdded)
	index := len(chunkServerInstance.Storage)

	reply := models.SuccessJSON{
		FileID:    chunkAdded.ChunkHandle,
		LastIndex: index,
	}

	log.Printf("[ChunkServer %d: AddChunk] Reply: %v\n", chunkServerInstance.PortNum, reply)

}

func TestUpdateChunk(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var chunk models.Chunk
	portNumber := 9000
	chunkServerInstance := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	var updatedChunk models.Chunk
	for idx, val := range chunkServerInstance.Storage {
		if val.ChunkHandle == chunk.ChunkHandle {
			chunkServerInstance.Storage[idx] = chunk
			updatedChunk = chunkServerInstance.Storage[idx]
			break
		}
	}
	reply := updatedChunk
	log.Printf("[ChunkServer %d: UpdateChunk] Reply: %v\n", chunkServerInstance.PortNum, reply)

}

func TestDeleteChunk(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var chunk models.Chunk
	portNumber := 9000
	chunkServerInstance := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	var deletedChunk models.Chunk
	for idx, val := range chunkServerInstance.Storage {
		if val.ChunkHandle == chunk.ChunkHandle {
			chunkServerInstance.Storage = append(chunkServerInstance.Storage[:idx], chunkServerInstance.Storage[:idx+1]...)
			deletedChunk = chunk
			break
		}
	}
	reply := deletedChunk
	log.Printf("[ChunkServer %d: DeleteChunk] Reply: %v\n", chunkServerInstance.PortNum, reply)

}
func Test_CreateFileChunks(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var successResponse models.SuccessJSON
	var chunk1 models.Chunk
	var chunk2 models.Chunk
	args := []models.Chunk{chunk1, chunk2}
	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}

	log.Println("============== CREATE CHUNKS IN CHUNK SERVER ==============")
	logMessage := fmt.Sprintf("[ChunkServer %d: CreateFileChunks] Chunks added: ", cs.PortNum)

	for _, c := range args {
		cs.Storage = append(cs.Storage, c)

		newChunk := models.Chunk{
			ChunkHandle: c.ChunkHandle,
			ChunkIndex:  c.ChunkIndex,
			Data:        c.Data,
		}

		replicateChunk := models.Replication{
			Port:  cs.PortNum,
			Chunk: newChunk,
		}

		client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
		if err != nil {
			log.Printf("[ChunkServer %d: CreateFileChunks] Dialing error: %v\n", cs.PortNum, err)
		}

		err = client.Call("MasterNode.Replication", replicateChunk, &successResponse)
		if err != nil {
			log.Printf("[ChunkServer %d: CreateFileChunks] Error calling RPC method: %v\n", cs.PortNum, err)
		}
		client.Close()

		log.Printf("[ChunkServer %d: CreateFileChunks] Successful Replication: %v\n", cs.PortNum, successResponse)

		logMessage += fmt.Sprintf("\n{%v %d %s}", c.ChunkHandle, c.ChunkIndex, helper.TruncateOutput(c.Data))
	}
	log.Printf("[ChunkServer %d: CreateFileChunks] Chunks: %s\n", cs.PortNum, logMessage)

	index := len(cs.Storage)

	reply := models.SuccessJSON{
		FileID:    args[0].ChunkHandle,
		LastIndex: index,
	}
	log.Printf("[ChunkServer %d: CreateFileChunks] Reply: %v\n", cs.PortNum, reply)

}

// Test Lease Functions
func TestGetLease(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	cs.Lease = models.Lease{
		Owner:      9000,
		Expiration: time.Time{},
		IsExpired:  false,
	}
	cs.LeaseExpiryChan = make(chan bool, 1)
	reply := cs.PortNum
	log.Printf("[ChunkServer %d: GetLease] Reply: %v\n", cs.PortNum, reply)
}

func TestRefreshLease(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	cs.Lease.Expiration = time.Time{}
	cs.Lease.IsExpired = false
	reply := cs.PortNum
	log.Printf("[ChunkServer %d: RefreshLease] Reply: %v\n", cs.PortNum, reply)
}

func TestRevokeLease(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	log.Printf("[ChunkServer %d] Lease as primary replica revoked\n", cs.PortNum)
	cs.Lease.IsExpired = true
	reply := cs.PortNum
	log.Printf("[ChunkServer %d: RevokeLease] Reply: %v\n", cs.PortNum, reply)
}

// Test ChunkServer Functions
func TestSendHeartbeat(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	heartBeat := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        "Alive",
		Node:          helper.CHUNK_SERVER_START_PORT,
		Port:          cs.PortNum,
	}
	if !cs.Lease.IsExpired {
		heartBeat.IsPrimaryReplica = true
	} else {
		heartBeat.IsPrimaryReplica = false
	}
	reply := heartBeat
	log.Printf("[ChunkServer %d: SendHeartbeat] Reply: %v\n", cs.PortNum, reply)
}

func TestReadRange(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	var dataStream []byte

	var args models.ReadData

	chunkUUID := args.ChunkMetadata.Handle

	if args.ChunkIndex1 == args.ChunkIndex2 {
		for _, chunk := range cs.Storage {
			if chunk.ChunkHandle == chunkUUID && chunk.ChunkIndex == args.ChunkIndex1 {
				dataStream = append(dataStream, chunk.Data...)
			}
		}
	} else {
		for _, chunk := range cs.Storage {
			if chunk.ChunkHandle == chunkUUID && chunk.ChunkIndex >= args.ChunkIndex1 && chunk.ChunkIndex <= args.ChunkIndex2 {
				dataStream = append(dataStream, chunk.Data...)
			}
		}
	}

	reply := dataStream
	log.Printf("[ChunkServer %d: ReadRange] Reply: %v\n", cs.PortNum, reply)
}

func TestInitialiseChunks(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
	logx.Logf("Initialised chunkServer Data", logx.FGBLUE, logx.BGWHITE)
	log.Printf("[Chunkserver %d: InitialiseChunks] Initialised Chunkserver data", portNumber)
	uuid1 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")

	chunk1 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  0,
		Data:        []byte("Hello"),
	}
	chunk2 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  1,
		Data:        []byte("World"),
	}
	chunk3 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  2,
		Data:        []byte("Foo"),
	}
	chunk4 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  3,
		Data:        []byte("Bar"),
	}

	cs.Storage = append(cs.Storage, chunk1, chunk2, chunk3, chunk4)
	log.Printf("[ChunkServer %d: InitialiseChunks] Stored data: %v\n", cs.PortNum, cs.Storage)
}

func TestRegistration(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var response string
	portNumber := 9000
	cs := &chunkserver.ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Printf("[ChunkServer %d: Registratiion] Dialing error: %v\n", cs.PortNum, err)
	}

	err = client.Call("MasterNode.RegisterChunkServers", cs.PortNum, &response)
	if err != nil {
		log.Printf("[ChunkServer %d: Registration] Error calling RPC method: %v\n", cs.PortNum, err)
	}
	client.Close()

	log.Printf("[ChunkServer %d: Registration]Registration Response %s\n", cs.PortNum, response)

}
