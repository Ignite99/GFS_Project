package main

import (
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

		time.Sleep(60 * time.Second)
	}
}

// Replicates chunk to all existing chunkServers
// Since all chunkServers anytime they add a chunk they should just replicate
func (mn *MasterNode) Replication(args models.Chunk, reply *models.SuccessJSON) error {
	var output models.SuccessJSON
	var response models.ChunkMetadata
	var filename string

	aliveNodes := make(map[int]string)

	// Find all alive nodes
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
			log.Println("Error connecting to RPC server:", err)
		}

		addChunkArgs := models.Chunk{
			ChunkHandle: args.ChunkHandle,
			ChunkIndex:  args.ChunkIndex,
			Data:        args.Data,
		}

		// Reply with file uuid & last index of chunk
		err = client.Call("ChunkServer.AddChunk", addChunkArgs, &output)
		if err != nil {
			log.Println("Error calling RPC method: ", err)
		}
		client.Close()

		response = models.ChunkMetadata{
			Handle:   output.FileID,
			Location: output.LastIndex,
		}
	}

	log.Println("Replication complete to all alive nodes: ", aliveNodes)

	mn.Chunks.Range(func(key, value interface{}) bool {
		if metadata, ok := value.(models.ChunkMetadata); ok {
			if metadata.Handle == args.ChunkHandle {
				filename = key.(string)
				return false
			}
		}
		return true
	})

	mn.Chunks.Store(filename, response)

	log.Println("MasterNode updated of new last index: ", response)

	*reply = output
	return nil
}

func (mn *MasterNode) Append(args models.Append, reply *models.AppendData) error {
	var appendArgs models.AppendData

	// The master node receives the client's request for appending data to the file and processes it.
	// It verifies that the file exists and handles any naming conflicts or errors.
	appendFile, ok := mn.Chunks.Load(args.Filename)
	if !ok {
		// If cannot be found generate new file, call create file
		// mn.CreateFile()
	}

	// Provides the client with information about the last chunk of the file
	chunkMetadata, ok := appendFile.(models.ChunkMetadata)
	if !ok {
		log.Println("Error from append: ", helper.ErrInvalidMetaData)
	}

	appendArgs = models.AppendData{ChunkMetadata: chunkMetadata, Data: args.Data}

	*reply = appendArgs

	return nil
}

func (mn *MasterNode) CreateFile(args models.CreateData, reply *models.ChunkMetadata) error {

	uuidNew := uuid.NewV4()

	metadata := models.ChunkMetadata{
		Handle:   uuidNew,
		Location: args.NumberOfChunks,
	}

	mn.Chunks.Store(args.Append.Filename, metadata)

	*reply = models.ChunkMetadata{
		Handle:   uuidNew,
		Location: args.NumberOfChunks,
	}
	return nil
}

func (mn *MasterNode) CreateLease() {

}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	gfsMasterNode := new(MasterNode)
	gfsMasterNode.InitializeChunkInfo()
	gfsMasterNode.InitializeAckMap()
	rpc.Register(gfsMasterNode)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Println("Error starting RPC server:", err)
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
