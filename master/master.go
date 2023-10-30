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

var clientAddr string

type MasterNode struct {
	ChunkInfo map[string]map[int]models.ChunkMetadata
	Chunks    sync.Map
	Mu        sync.Mutex
}

// Used for read for chunkserver
func (mn *MasterNode) GetChunkLocation(args models.GetChunkLocationArgs, reply *models.ChunkMetadata) error {
	key := args.Filename + "_" + strconv.Itoa(args.ChunkIndex)

	// Loads the filename + chunk index to load metadata from key
	value, ok := mn.Chunks.Load(key)
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

		time.Sleep(30 * time.Second)
	}
}

// Master to select a set of Chunk Servers to store replicas of a particular chunk
// this function will also process files that are created for the first time (and have not been stored in Chunk Servers)
func (mn *MasterNode) CreateReplicas() {
	log.Println("======== Starting Replication ========")
	aliveNodes := make(map[int]string)

	helper.AckMap.Range(func(key, value interface{}) bool {
		if val, ok := value.(string); ok && val == "alive" {
			if port, ok := key.(int); ok {
				aliveNodes[port] = val
			}
		}
		return true
	})

	for port, _ := range aliveNodes {
		client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
		if err != nil {
			log.Fatal("Error connecting to RPC server:", err)
		}
		defer client.Close()

		args := models.Chunk{
			ChunkHandle: uuid.NewV4(),
			Data:        []int{},
		}
		var reply models.Chunk

		err = client.Call("ChunkServer.AddChunk", args, &reply)
		if err != nil {
			log.Fatal("Error calling RPC method: ", err)
		}

		fmt.Printf("This is the chunkHandle: %v, data: %v\n", reply.ChunkHandle, reply.Data)
	}
}

func (mn *MasterNode) Append() {

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

	gfsMasterNode.CreateReplicas()
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

		clientAddr = conn.RemoteAddr().String()
		fmt.Println("Client Address: ", clientAddr)
		gfsMasterNode.Mu.Lock()
		helper.ChunkServers[clientAddr] = &models.ChunkServerState{
			LastHeartbeat: time.Now(),
			Status:        "alive",
		}
		gfsMasterNode.Mu.Unlock()
		go rpc.ServeConn(conn)
	}
}

/* ============================================ INITIALISING DATA  ===========================================*/

func (mn *MasterNode) InitializeChunkInfo() {

	uuid1 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")
	uuid2 := helper.StringToUUID("a45a0e0e-68d0-493f-8771-f7947cc9217e")
	uuid3 := helper.StringToUUID("61acd4ca-0ca5-4ba7-b827-dbe81e7529d4")
	uuid4 := helper.StringToUUID("a49a0e0e-68d0-493f-8771-f7947cc9217e")

	// Handle is the uuid of the metadata that has been assigned to the chunk
	// Location is the chunkServer that it is located in
	metadata1 := models.ChunkMetadata{
		Handle:   uuid1,
		Location: 1,
	}

	// The reason why it has a underscore 0 in its naming is to indicate the chunk index.
	// Thus if file1.txt_0. It is the first chunk of file1.txt
	mn.Chunks.Store("file1.txt_0", metadata1)

	metadata2 := models.ChunkMetadata{
		Handle:   uuid2,
		Location: 1,
	}
	mn.Chunks.Store("file1.txt_1", metadata2)

	metadata3 := models.ChunkMetadata{
		Handle:   uuid3,
		Location: 1,
	}
	mn.Chunks.Store("file2.txt_0", metadata3)

	metadata4 := models.ChunkMetadata{
		Handle:   uuid4,
		Location: 1,
	}
	mn.Chunks.Store("file2.txt_1", metadata4)
}

func (mn *MasterNode) InitializeAckMap() {
	helper.AckMap.Store(helper.CHUNK_SERVER_START_PORT, "alive")

	helper.ChunkServerPorts = append(helper.ChunkServerPorts, helper.CHUNK_SERVER_START_PORT)
}
