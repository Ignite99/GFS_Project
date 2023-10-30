package main // should be client, set temporarily as main so it can be run

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
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
		log.Fatal("Dialing error", err)
	}
	defer client.Close()

	// Following the API in chunk server, but needs to be updated to follow the GFS spec
	var reply models.Chunk
	client.Call("ChunkServer.Read", metadata, &reply)
	fmt.Println("Received chunk: ", reply.Data)
}

// Append to a chunk in the chunk server
func AppendChunk() {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer client.Close()

	args := models.ChunkLocationArgs{
		Filename: "file1.txt",
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
// Will update so that it receives inputs and performs ops accordingly
func runClient() {
	chunkMetadata := RequestChunkLocation("file1", 0)
	ReadChunk(chunkMetadata)
	chunkMetadata = RequestChunkLocation("file1", 1)
	ReadChunk(chunkMetadata)

	// AppendChunk()
}

func main() {
	runClient()
}
