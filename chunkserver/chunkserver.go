package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

// data structure used for this GFS operation
// an Object is split into several Chunks
type Object []byte

// assume this is for a ChunkServer instance
type ChunkServer struct {
	// assume everything stored in storage within ChunkServer
	Location        int // same as location in chunkmetadata
	storage         []models.Chunk
	ChunkingStorage sync.Map
}

/* =============================== Chunk Storage functions =============================== */
// Master will talk with these Chunk functions to create, update, delete, or replicate chunks

// get chunk from storage
func (cs *ChunkServer) GetChunk(chunkHandle uuid.UUID, chunkLocation int) models.Chunk {
	var chunkRetrieved models.Chunk

	// Only iterate through those with relevant uuid. Will only enter upon detecting ChunkLocation is similar to chunkIndex
	for _, val := range cs.storage {
		if val.ChunkHandle == chunkHandle && val.ChunkIndex == chunkLocation {
			chunkRetrieved = val
			break
		}
	}

	return chunkRetrieved
}

// add new chunk to storage
func (cs *ChunkServer) AddChunk(args models.Chunk, reply *models.SuccessJSON) error {
	log.Println("============== ADDING CHUNK ==============")
	log.Println("Chunk added: ", args)
	cs.storage = append(cs.storage, args)
	index := len(cs.storage)

	*reply = models.SuccessJSON{
		FileID:    args.ChunkHandle,
		LastIndex: index,
	}
	return nil
}

// update value of chunk in storage
func (cs *ChunkServer) UpdateChunk(args models.Chunk, reply *models.Chunk) error {
	var updatedChunk models.Chunk
	for idx, val := range cs.storage {
		if val.ChunkHandle == args.ChunkHandle {
			cs.storage[idx] = args
			updatedChunk = cs.storage[idx]
			break
		}
	}
	*reply = updatedChunk
	return nil
}

// delete chunk from storage
func (cs *ChunkServer) DeleteChunk(args models.Chunk, reply *models.Chunk) error {
	var deletedChunk models.Chunk
	for idx, val := range cs.storage {
		if val.ChunkHandle == args.ChunkHandle {
			cs.storage = append(cs.storage[:idx], cs.storage[:idx+1]...)
			deletedChunk = args
			break
		}
	}
	*reply = deletedChunk
	return nil
}

/* ============================ ChunkServer functions ============================ */

// chunk server to reply heartbeat to master to verify that it is alive
func (cs *ChunkServer) SendHeartBeat(args models.ChunkServerState, reply *models.ChunkServerState) error {
	heartBeat := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        args.Status,
		Node:          helper.CHUNK_SERVER_START_PORT,
	}
	*reply = heartBeat
	return nil
}

// client to call this API when it wants to read data
func (cs *ChunkServer) Read(chunkMetadata models.ChunkMetadata, reply *models.Chunk) error {
	// will add more logic here
	ch := chunkMetadata.Handle
	for _, chunk := range cs.storage {
		if chunk.ChunkHandle == ch {
			fmt.Println(chunk.Data)
			*reply = chunk
		}
	}

	return nil
}

// client to call this API when it wants to append data
func (cs *ChunkServer) Append(args models.AppendData, reply *models.Chunk) error {

	var newChunk models.Chunk
	var successResponse models.SuccessJSON

	log.Println("Appending starting")

	chunk := cs.GetChunk(args.Handle, args.Location)
	chunk.Data = append(chunk.Data, args.Data...)

	// Adds chunk in chunk server for data that overflowed
	if len(chunk.Data) > helper.CHUNK_SIZE {
		exceedingData := []byte{}
		for _, num := range chunk.Data {
			if len(chunk.Data) < 64 {
				// Add the number to the exceedingNumbers slice if it's within the first 64 elements.
				exceedingData = append(exceedingData, num)
			}
		}

		chunk.Data = chunk.Data[:64]

		newChunk = models.Chunk{
			ChunkHandle: args.Handle,
			ChunkIndex:  chunk.ChunkIndex + 1,
			Data:        exceedingData,
		}

		cs.AddChunk(newChunk, nil)
	}

	// Updates Master for new last index entry
	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Println("Dialing error: ", err)
	}

	err = client.Call("MasterNode.UpdateLastIndex", chunk, &successResponse)
	if err != nil {
		log.Println("Error calling RPC method: ", err)
	}
	client.Close()

	log.Println("Successful Last Index Update: ", successResponse)

	*reply = chunk
	return nil
}

// client to call this API when it wants to truncate data
// func (cs *ChunkServer) Truncate(chunkMetadata models.ChunkMetadata, reply *models.Chunk) error {
// 	// will add more logic here
// 	chunkToTruncate := cs.GetChunk(chunkMetadata.Handle)
// 	cs.DeleteChunk(chunkToTruncate, nil)

// 	*reply = chunkToTruncate
// 	return nil
// }

// master to call this when it needs to create new replica for a chunk
func (cs *ChunkServer) CreateNewReplica() {
	chunkServerReplica := new(ChunkServer)
	rpc.Register(chunkServerReplica)
	chunkServerReplica.storage = cs.storage
}

// master to call this to pass lease to chunk server
func (cs *ChunkServer) ReceiveLease() {

}

// command or API call for MAIN function to run chunk server
func runChunkServer() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	chunkServerInstance := new(ChunkServer)
	rpc.Register(chunkServerInstance)

	chunkServerInstance.InitialiseChunks()

	// start RPC server for chunk server (refer to Go's RPC documentation for more details)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.CHUNK_SERVER_START_PORT))

	if err != nil {
		log.Fatal("Error starting RPC server", err)
	}
	defer listener.Close()

	log.Printf("RPC listening on port %d", helper.CHUNK_SERVER_START_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Error accepting connection", err)
		}
		// serve incoming RPC requests
		go rpc.ServeConn(conn)
	}
}

// starting function for this file --> will be moved to main.go
func main() {
	runChunkServer()
}

func (cs *ChunkServer) InitialiseChunks() {
	fmt.Println("Initialised chunkServer Data")
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

	cs.storage = append(cs.storage, chunk1, chunk2, chunk3, chunk4)
}
