package main // should be client, set temporarily as main so it can be run

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"math/rand"
	"time"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

type Task struct {
	Operation int
	Filename string
	Filesize int
}

const (
	READ = iota
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
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Error connecting to RPC server: ", err)
	}
	defer client.Close()

	args := models.ChunkLocationArgs{
		Filename: filename,
	}
	var reply models.ChunkMetadata

	// Dont need call chunk index, if client knew chunk index that would be weird
	err = client.Call("MasterNode.Append", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}
}

func createFile() {}

// Start the client, to be called from main.go
func StartClients() {
	task1 := Task{
		Operation: READ,
		Filename: FILE1,
		Filesize: File1Size,
	}

	task2 := Task{
		Operation: APPEND,
		Filename: FILE1,
		Filesize: File1Size,
	}

	go runClient(task1)
	go runClient(task2)
}
func runClient(t Task) {

	chunks := t.Filesize / helper.CHUNK_SIZE + 1
	if t.Operation == READ {
		for i := 0; i < chunks; i++ {
			chunkMetadata := RequestChunkLocation(t.Filename, i)
			ReadChunk(chunkMetadata)
		}
	} else if t.Operation == APPEND {
		dataSize := rand.Intn(helper.CHUNK_SIZE)
		data := make([]int, dataSize)
		for i := 0; i < dataSize; i++ {
			data[i] = rand.Intn(100)
		}
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
	rand.Seed(time.Now().UnixNano())
	StartClients()
}
