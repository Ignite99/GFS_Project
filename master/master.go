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

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

type MasterNode struct {
	ChunkInfo       map[string]map[int]models.ChunkMetadata
	Chunks          sync.Map
	StorageLocation sync.Map
	Mu              sync.Mutex
}

// Used for read for chunkserver
func (mn *MasterNode) GetChunkLocation(args models.ChunkLocationArgs, reply *models.ChunkMetadata) error {
	// Loads the filename + chunk index to load metadata from key
	value, ok := mn.Chunks.Load(args.Filename)
	if !ok {
		return helper.ErrChunkNotFound
	}

	chunkMetadata, ok := value.(models.ChunkMetadata)
	if !ok {
		return helper.ErrInvalidMetaData
	}

	*reply = chunkMetadata

	return nil
}

func HeartBeatManager(port int) models.ChunkServerState {
	var reply models.ChunkServerState

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Print("Dialing error: ", err)
		return reply
	}
	defer client.Close()

	heartBeatRequest := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        "alive",
	}

	log.Println("Send heartbeat request to chunk")
	client.Call("ChunkServer.SendHeartBeat", heartBeatRequest, &reply)
	log.Println("Received heartbeat reply from Chunk Server with info:", reply)
	return reply
}

// Will iterate through all chunk servers initialised and ping server with heartbeatManager
func HeartBeatTracker() {
	for {
		for _, port := range helper.ChunkServerPorts {
			output := HeartBeatManager(port)
			if output.Status != "alive" {
				helper.AckMap.Store(port, "dead")

				log.Printf("Chunk Server at port %d is dead", port)
			}
		}

		time.Sleep(3 * time.Second)
	}
}

// Follow up of append operation
// Master to select a set of Chunk Servers to store replicas of a particular chunk
// this function will also process files that are created for the first time (and have not been stored in Chunk Servers)
func (mn *MasterNode) Replication(args models.Replication) models.ReplicationResponse {
	var output models.SuccessJSON
	var response models.ChunkMetadata

	fmt.Println("======== Starting Replication ========")
	aliveNodes := make(map[int]string)
	ChunkLastIndexLocations := make(map[int]int)

	helper.AckMap.Range(func(key, value interface{}) bool {
		if val, ok := value.(string); ok && val == "alive" {
			if port, ok := key.(int); ok {
				aliveNodes[port] = val
			}
		}
		return true
	})

	// Copies data to all alive nodes at that point
	for port, _ := range aliveNodes {
		client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
		if err != nil {
			log.Fatal("Error connecting to RPC server:", err)
		}
		defer client.Close()

		addChunkArgs := models.Chunk{
			ChunkHandle: args.Chunk.ChunkHandle,
			Data:        args.Chunk.Data,
		}

		// Reply with file uuid & last index
		err = client.Call("ChunkServer.AddChunk", addChunkArgs, &output)
		if err != nil {
			log.Fatal("Error calling RPC method: ", err)
		}

		response = models.ChunkMetadata{
			Handle:   output.FileID,
			Location: output.LastIndex,
		}

		mn.Chunks.Store(args.Filename, response)
		ChunkLastIndexLocations[port] = output.LastIndex
	}

	replicationResponse := models.ReplicationResponse{
		FileName:        args.Filename,
		StorageLocation: ChunkLastIndexLocations,
	}
	return replicationResponse
}

func (mn *MasterNode) UpdateLastIndex(args models.Chunk, reply *models.SuccessJSON) {
	fmt.Println("Updating Last index")

	newMetadata := models.ChunkMetadata{
		Handle:   args.ChunkHandle,
		Location: args.ChunkIndex,
	}

	mn.Chunks.Store("file1.txt", newMetadata)

	log.Println("Updated Last Index")
	*reply = models.SuccessJSON{
		FileID:    args.ChunkHandle,
		LastIndex: args.ChunkIndex,
	}
}

func (mn *MasterNode) Append(args models.Append, reply *models.AppendData) {
	var appendArgs models.AppendData
	// var appendResponse models.Chunk

	// The master node receives the client's request for appending data to the file and processes it.
	// It verifies that the file exists and handles any naming conflicts or errors.
	appendFile, ok := mn.Chunks.Load(args.Filename)
	if !ok {
		// If cannot be found generate new file, call create file
		mn.CreateFile()
	}

	// Provides the client with information about the last chunk of the file
	chunkMetadata, ok := appendFile.(models.ChunkMetadata)
	if !ok {
		log.Println("Error from append: ", helper.ErrInvalidMetaData)
	}

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.CHUNK_SERVER_START_PORT))
	if err != nil {
		log.Print("Dialing error: ", err)
	}
	client.Close()

	appendArgs = models.AppendData{ChunkMetadata: chunkMetadata, Data: args.Data}

	*reply = appendArgs
}

func (mn *MasterNode) CreateFile() {

}

// Master to create lease for chunk servers
func (mn *MasterNode) CreateLease() {

}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	gfsMasterNode := new(MasterNode)
	gfsMasterNode.InitializeChunkInfo()
	gfsMasterNode.InitializeAckMap()
	rpc.Register(gfsMasterNode)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}
	defer listener.Close()

	go HeartBeatTracker()

	log.Printf("RPC server is listening on port %d", helper.MASTER_SERVER_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go rpc.ServeConn(conn)
	}
}

/* ============================================ INITIALISING DATA  ===========================================*/

func (mn *MasterNode) InitializeChunkInfo() {

	uuid1 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")

	// Handle is the uuid of the metadata that has been assigned to the chunk
	// Location is the chunkServer that it is located in
	metadata1 := models.ChunkMetadata{
		Handle: uuid1,
		// Last index of file, based on ChunkIndex in models.Chunk
		Location: 3,
	}

	mn.Chunks.Store("file1.txt", metadata1)
}

func (mn *MasterNode) InitializeAckMap() {
	helper.AckMap.Store(helper.CHUNK_SERVER_START_PORT, "alive")

	helper.ChunkServerPorts = append(helper.ChunkServerPorts, helper.CHUNK_SERVER_START_PORT)
}
