package main // should be client, set temporarily as main so it can be run

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

// duplicated from chunk server to prevent breakage, to be removed
type Chunk struct {
	chunkHandle int
	data        []int
}

// Request chunk location from master server
func requestChunkLocation(filename string, chunkIndex int) models.ChunkMetadata {
	client, err := rpc.Dial("tcp", ":" + strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Dialing error", err)
	}
	defer client.Close()

	chunkRequest := models.GetChunkLocationArgs {
		Filename: filename,
		ChunkIndex: chunkIndex,
	}
	var reply models.ChunkMetadata
	client.Call("MasterNode.GetChunkLocation", chunkRequest, &reply)
	return reply
}

// Read from a chunk in the chunk server
func readChunk(metadata models.ChunkMetadata) {
	client, err := rpc.Dial("tcp", ":" + strconv.Itoa(helper.CHUNK_SERVER_START_PORT))
	if err != nil {
		log.Fatal("Dialing error", err)
	}
	defer client.Close()

	// Following the API in chunk server, but needs to be updated to follow the GFS spec
	var reply Chunk
	client.Call("ChunkServer.Read", metadata, &reply)
	fmt.Println("Received chunk: ", reply.data)
}

// Append to a chunk in the chunk server
func appendChunk(metadata models.ChunkMetadata) {
	// TODO
	return
}

// Start the client, to be called from main.go
// Will update so that it receives inputs and performs ops accordingly
func runClient() {
	chunkMetadata := requestChunkLocation("file1", 0)
	readChunk(chunkMetadata)
	chunkMetadata = requestChunkLocation("file1", 1)
	readChunk(chunkMetadata)
}

func main() {
	runClient()
}
