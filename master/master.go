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

type MasterNode struct {
	ChunkInfo       map[string]models.ChunkMetadata
	Chunks          sync.Map
	StorageLocation sync.Map
	Mu              sync.Mutex
}

// Used for read for chunkserver
func (mn *MasterNode) GetChunkLocation(args models.ChunkLocationArgs, reply *models.ChunkMetadata) error {
	// Loads the filename + chunk index to load metadata from key
	chunkMetadata := mn.ChunkInfo[args.Filename]
	if chunkMetadata.Location == 0 {
		return helper.ErrChunkNotFound
	}

	*reply = chunkMetadata

	return nil
}

func HeartBeatManager(port int) models.ChunkServerState {
	var reply models.ChunkServerState

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Print("[Master] Dialing error: ", err)
		return reply
	}
	defer client.Close()

	heartBeatRequest := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        "alive",
	}

	log.Println("[Master] Send heartbeat request to chunk")
	err = client.Call("ChunkServer.SendHeartBeat", heartBeatRequest, &reply)
	if err != nil {
		log.Println("[Master] Error calling RPC method: ", err)
	}

	log.Printf("[Master] Heartbeat from ChunkServer %d, Status: %s\n", reply.Port, reply.Status)
	return reply
}

// Will iterate through all chunk servers initialised and ping server with heartbeatManager
func HeartBeatTracker() {
	for {
		for _, port := range helper.ChunkServerPorts {
			output := HeartBeatManager(port)
			if output.Status != "alive" {
				helper.AckMap.Store(port, "dead")

				log.Printf("[Master] Chunk Server at port %d is dead\n", port)
			}
		}

		time.Sleep(60 * time.Second)
	}
}

// Replicates chunk to all existing chunkServers
// Since all chunkServers anytime they add a chunk they should just replicate
func (mn *MasterNode) Replication(args models.Replication, reply *models.SuccessJSON) error {
	var output models.SuccessJSON
	var response models.ChunkMetadata
	var filename string

	log.Println("Replication started")

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
		if port != args.Port {
			fmt.Println("[Master] Replicating to port: ", port)

			client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
			if err != nil {
				log.Println("[Master] Error connecting to RPC server:", err)
			}
      
			addChunkArgs := models.Chunk{
				ChunkHandle: args.Chunk.ChunkHandle,
				ChunkIndex:  args.Chunk.ChunkIndex,
				Data:        args.Chunk.Data,
			}

			// Reply with file uuid & last index of chunk
			err = client.Call("ChunkServer.AddChunk", addChunkArgs, &output)
			if err != nil {
				log.Println("[Master] Error calling RPC method: ", err)
			}
			client.Close()

			response = models.ChunkMetadata{
				Handle:   output.FileID,
				Location: output.LastIndex,
			}
		}
	}

	log.Println("[Master] Replication complete to all alive nodes: ", aliveNodes)

	mn.Chunks.Range(func(key, value interface{}) bool {
		if metadata, ok := value.(models.ChunkMetadata); ok {
			if metadata.Handle == args.Chunk.ChunkHandle {
				filename = key.(string)
				return false
			}
		}
		return true
	})

	mn.ChunkInfo[filename] = response

	log.Println("[Master] MasterNode updated of new last index: ", response)

	*reply = output
	return nil
}

func (mn *MasterNode) Append(args models.Append, reply *models.AppendData) error {
	var appendArgs models.AppendData

	// The master node receives the client's request for appending data to the file and processes it.
	// It verifies that the file exists and handles any naming conflicts or errors.
	appendFile := mn.ChunkInfo[args.Filename]
	if appendFile.Location == 0 {
		// If cannot be found generate new file, call create file
		// mn.CreateFile()
		log.Println("[Master] File does not exist")
	}

	appendArgs = models.AppendData{ChunkMetadata: appendFile, Data: args.Data}

	*reply = appendArgs

	return nil
}

func (mn *MasterNode) CreateFile(args models.CreateData, reply *models.ChunkMetadata) error {

	if _, exists := mn.ChunkInfo[args.Append.Filename]; !exists {
		log.Println("Key does not exist in the map")

		uuidNew := uuid.NewV4()

		metadata := models.ChunkMetadata{
			Handle:    uuidNew,
			Location:  helper.CHUNK_SERVER_START_PORT,
			LastIndex: args.NumberOfChunks-1,
		}

		mn.ChunkInfo[args.Append.Filename] = metadata

		*reply = models.ChunkMetadata{
			Handle:    uuidNew,
			Location:  metadata.Location,
			LastIndex: metadata.LastIndex,
		}
		return nil
	}

	*reply = models.ChunkMetadata{}

	return nil
}

func (mn *MasterNode) CreateLease() {

}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("[Master] Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	gfsMasterNode := &MasterNode{
		ChunkInfo: make(map[string]models.ChunkMetadata),
	}
	gfsMasterNode.InitializeChunkInfo()

	rpc.Register(gfsMasterNode)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Println("[Master] Error starting RPC server:", err)
	}
	defer listener.Close()

	go HeartBeatTracker()

	log.Printf("[Master] RPC server is listening on port %d\n", helper.MASTER_SERVER_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[Master] Error accepting connection: %v", err)
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
		Handle:    uuid1,
		Location:  8090,
		LastIndex: 3,
	}

	mn.ChunkInfo["file1.txt"] = metadata1
}

func (mn *MasterNode) RegisterChunkServers(args int, reply *string) error {
	fmt.Println("Registering port: ", args)

	helper.AckMap.Store(args, "alive")

	helper.ChunkServerPorts = append(helper.ChunkServerPorts, args)

	*reply = "Success"

	return nil
}
