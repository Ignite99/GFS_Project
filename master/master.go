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

	"github.com/google/uuid"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

var clientAddr string

type MasterNode struct {
	ChunkInfo map[string]map[int]models.ChunkMetadata
	Chunks    sync.Map
	Mu        sync.Mutex
}

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

// heartbeat only need to send from Chunk Server to Master, no need client send
// Somehow the client address port keeps on changing, or maybe unless i change it? To instead just post the a static string
func (mn *MasterNode) HeartBeatManager() {
	mn.Mu.Lock()
	defer mn.Mu.Unlock()
	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.CHUNK_SERVER_START_PORT))
	if err != nil {
		log.Fatal("Dialing error", err)
	}
	defer client.Close()

	heartBeatRequest := models.ChunkServerInfo{
		LastHeartbeat: time.Now(),
		Status:        "alive",
	}
	var reply models.ChunkServerInfo
	log.Println("Send heartbeat request to chunk")
	client.Call("ChunkServer.SendHeartBeat", heartBeatRequest, &reply)
	log.Println("Received heartbeat reply from Chunk Server with info:", reply)
	return

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
	rpc.Register(gfsMasterNode)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}
	defer listener.Close()

	log.Printf("RPC server is listening on port %d", helper.MASTER_SERVER_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// // Not too sure how I am going to ensure client address stays the same haiz
		clientAddr = conn.RemoteAddr().String()
		fmt.Println("Client Address: ", clientAddr)
		gfsMasterNode.Mu.Lock()
		helper.ChunkServers[clientAddr] = &models.ChunkServerInfo{
			LastHeartbeat: time.Now(),
			Status:        "alive",
		}
		gfsMasterNode.Mu.Unlock()
		// send heartbeat
		gfsMasterNode.HeartBeatManager()
		go rpc.ServeConn(conn)
	}
}

/* ============================================ INITIALISING DATA  ===========================================*/

func (mn *MasterNode) InitializeChunkInfo() {
	// Handle is the uuid of the metadata that has been assigned to the chunk
	// Location is the chunkServer that it is located in
	metadata1 := models.ChunkMetadata{
		Handle:   uuid.New(),
		Location: 1,
	}

	// The reason why it has a underscore 0 in its naming is to indicate the chunk index.
	// Thus if file1.txt_0. It is the first chunk of file1.txt
	mn.Chunks.Store("file1.txt_0", metadata1)

	metadata2 := models.ChunkMetadata{
		Handle:   uuid.New(),
		Location: 2,
	}
	mn.Chunks.Store("file1.txt_1", metadata2)

	metadata3 := models.ChunkMetadata{
		Handle:   uuid.New(),
		Location: 1,
	}
	mn.Chunks.Store("file2.txt_1", metadata3)

	metadata4 := models.ChunkMetadata{
		Handle:   uuid.New(),
		Location: 1,
	}
	mn.Chunks.Store("file2.txt_2", metadata4)
}
