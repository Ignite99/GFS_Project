package main // should be client, set temporarily as main so it can be run

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

type Task struct {
	Operation int
	Filename  string
	Filesize  int
}

const (
	READ   = iota
	APPEND = iota

	FILE1 = "file1.txt"
	FILE2 = "file2.txt"
	FILE3 = "file3.txt"
)

var (
	File1Size = 65
	File2Size = 10
	File3Size = 120
)

// Request chunk location from master server
func RequestChunkLocation(filename string, chunkIndex int) models.ChunkMetadata {
	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Dialing error", err)
	}
	defer client.Close()

	chunkRequest := models.ChunkLocationArgs{
		Filename:   filename,
		ChunkIndex: chunkIndex,
	}
	var reply models.ChunkMetadata
	client.Call("MasterNode.GetChunkLocation", chunkRequest, &reply)
	return reply
}

// Read from a chunk in the chunk server
func ReadChunk(metadata models.ChunkMetadata) {
	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.CHUNK_SERVER_START_PORT))
	if err != nil {
		log.Fatal("Error connecting to RPC server: ", err)
	}
	defer client.Close()

	// Following the API in chunk server, but needs to be updated to follow the GFS spec
	var reply models.Chunk
	err = client.Call("ChunkServer.Read", metadata, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}
	fmt.Println("Received chunk: ", reply.Data)
}

// Append to a chunk in the chunk server
func AppendChunk(filename string, data []int) {
	var appendReply models.AppendData
	var reply models.Chunk
	// var replicationReply models.ReplicationResponse

	client1, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Error connecting to RPC server: ", err)
	}

	appendArgs := models.Append{Filename: filename, Data: data}

	// Sends a request to the master node. This request includes the file name it wants to append data to.
	err = client1.Call("MasterNode.Append", appendArgs, &appendReply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}
	client1.Close()

	client2, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(helper.CHUNK_SERVER_START_PORT))
	if err != nil {
		log.Fatal("Error connecting to RPC server: ", err)
	}
	err = client2.Call("ChunkServer.Append", appendReply, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}
	client2.Close()

	log.Println("Successfully appended payload: ", reply)
}

func CreateFile() {}

// Start the client, to be called from main.go
func StartClients() {
	fmt.Println("======== RUNNING CLIENTS ========")

	task1 := Task{
		Operation: READ,
		Filename:  FILE1,
		Filesize:  File1Size,
	}

	task2 := Task{
		Operation: APPEND,
		Filename:  FILE1,
		Filesize:  File1Size,
	}

	runClient(task1)
	runClient(task2)
}
func runClient(t Task) {

	chunks := t.Filesize/helper.CHUNK_SIZE + 1
	if t.Operation == READ {
		for i := 0; i < chunks; i++ {
			chunkMetadata := RequestChunkLocation(t.Filename, i)
			ReadChunk(chunkMetadata)
		}
	} else if t.Operation == APPEND {
		dataSize := rand.Intn(helper.CHUNK_SIZE)
		fmt.Println("Data size: ", dataSize)
		data := make([]int, dataSize)
		for i := 0; i < dataSize; i++ {
			data[i] = rand.Intn(100)
		}
		fmt.Println("Data: ", data)
		AppendChunk(t.Filename, data)
	}
	/*
		chunkMetadata := RequestChunkLocation("file1", 0)
		ReadChunk(chunkMetadata)
		chunkMetadata = RequestChunkLocation("file1", 1)
		ReadChunk(chunkMetadata)

		AppendChunk("file1.txt")
	*/
}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	rand.Seed(time.Now().UnixNano())
	StartClients()
}
