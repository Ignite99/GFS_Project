package client // should be client, set temporarily as main so it can be run

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

/* =============================== Chunk-related functions =============================== */

// Request chunk location from master server
func requestChunkLocation(filename string, chunkIndex int) models.ChunkMetadata {
	client := dial(helper.MASTER_SERVER_PORT)
	defer client.Close()

	chunkRequest := models.ChunkLocationArgs{
		Filename:   filename,
		ChunkIndex: chunkIndex,
	}
	var reply models.ChunkMetadata
	err := client.Call("MasterNode.GetChunkLocation", chunkRequest, &reply)
	if err != nil {
		log.Fatalln("[Client] Error calling RPC method:", err)
	}

	if chunkIndex != reply.LastIndex {
		log.Println("[Client] Last Index in arguments is incorrect LastIndex of chunkServer")
		log.Println("[Client] The last index of chunkServer is:", reply.LastIndex)
	}

	return reply
}

// Read from a chunk in the chunk server
func readChunks(metadata models.ChunkMetadata, index1 int, index2 int) []byte {
	client := dial(metadata.Location)
	defer client.Close()

	var reply []byte

	query := models.ReadData{
		ChunkIndex1:   index1,
		ChunkIndex2:   index2,
		ChunkMetadata: metadata,
	}

	err := client.Call("ChunkServer.ReadRange", query, &reply)
	if err != nil {
		log.Fatalln("[Client] Error calling RPC method:", err)
	}

	log.Println("[Client] Received chunk:", helper.TruncateOutput(reply))

	return reply
}

/* =============================== File-related functions =============================== */

func ReadFile(filename string, firstIndex int, LastIndex int) {
	// Compute number of chunks
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatalln("[Client] Error acquiring file information:", err)
	}
	chunks := computeNumberOfChunks(int(fi.Size()))

	// Read each chunk
	chunkMetadata := requestChunkLocation(filename, chunks)
	//chunkMetadata.Handle = helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")
	readChunks(chunkMetadata, firstIndex, LastIndex)
	// TODO: update local copy here
}

// Append to a chunk in the chunk server
// TODO: can append at any point within the existing file
func AppendToFile(filename string, data []byte) {
	appendArgs := models.Append{Filename: filename, Data: data}
	var appendReply models.AppendData
	// var replicationReply models.ReplicationResponse

	// Sends a request to the master node. This request includes the file name it wants to append data to.
	mnClient := dial(helper.MASTER_SERVER_PORT)
	err := mnClient.Call("MasterNode.Append", appendArgs, &appendReply)
	if err != nil {
		log.Fatalln("[Client] Error calling RPC method:", err)
	}
	mnClient.Close()

	fmt.Println("Masternode Append works")

	// Append data to chunks
	var reply models.Chunk
	csClient := dial(helper.CHUNK_SERVER_START_PORT)
	err = csClient.Call("ChunkServer.Append", appendReply, &reply)
	if err != nil {
		log.Fatalln("[Client] Error calling RPC method:", err)
	}
	csClient.Close()
	log.Println("[Client] Successfully appended payload:", helper.TruncateOutput(data))

	// Append data to local copy of file
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("[Client] Error opening file:", err)
	}
	_, err = f.Write(data)
	if err != nil {
		log.Println("[Client] Error writing to file:", err)
	}
	err = f.Close()
	if err != nil {
		log.Println("[Client] Error closing file:", err)
	}
}

func CreateFile(filename string, data []byte) error {
	fmt.Println("CREATING FILE")

	// Create the file locally
	err := os.WriteFile(filename, data, 0644)
	if err != nil {
		log.Println("[Client] Error writing to file:", err)
	}

	// Compute number of chunks and prepare args
	var metadata models.ChunkMetadata
	chunks := computeNumberOfChunks(len(data))
	createArgs := models.CreateData{
		Append:         models.Append{Filename: filename, Data: data},
		NumberOfChunks: chunks,
	}

	// Inform master server of new file
	mnClient := dial(helper.MASTER_SERVER_PORT)
	err = mnClient.Call("MasterNode.CreateFile", createArgs, &metadata)
	if err != nil {
		log.Fatalln("[Client] Error calling RPC method:", err)
	}
	mnClient.Close()

	if metadata.Location == 0 {
		log.Println("[Client] File already exists in chunkserver")
		return errors.New("file already exists in chunkserver")
	}

	// Split file data into chunks and prepare args
	// Reply has the chunk uuid and location(last index)
	var reply models.SuccessJSON
	var chunkData []byte
	chunkArray := make([]models.Chunk, chunks)
	for i := 0; i < chunks; i++ {
		if i == chunks-1 {
			chunkData = data
		} else {
			chunkData = data[:helper.CHUNK_SIZE]
			data = data[helper.CHUNK_SIZE:]
		}
		chunk := models.Chunk{
			ChunkHandle: metadata.Handle,
			ChunkIndex:  i,
			Data:        chunkData,
		}
		chunkArray[i] = chunk
	}

	// Push chunks to chunk server
	csClient := dial(helper.CHUNK_SERVER_START_PORT)
	err = csClient.Call("ChunkServer.CreateFileChunks", chunkArray, &reply)
	if err != nil {
		log.Fatalln("[Client] Error calling RPC method:", err)
	}
	csClient.Close()
  
	log.Println("[Client] Successfully created file in chunkserver:", reply)

	return nil
}

/* =============================== Helper functions =============================== */

func dial(address int) *rpc.Client {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(address))
	if err != nil {
		log.Fatalln("[Client] Error connecting to RPC server:", err)
	}
	return client
}

func computeNumberOfChunks(size int) int {
	chunks := size/helper.CHUNK_SIZE
	if (size % helper.CHUNK_SIZE > 0) {
		chunks++
	}
	return chunks
}

func main () {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("[Client] Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)
}
