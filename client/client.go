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
	DataSize  int
}

const (
	READ   = iota
	APPEND = iota
	WRITE  = iota

	FILE1 = "file1.txt"
	FILE2 = "file2.txt"
	FILE3 = "file3.txt"

	CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n"
)

/* =============================== Chunk-related functions =============================== */

// Request chunk location from master server
func RequestChunkLocation(filename string, chunkIndex int) models.ChunkMetadata {
	client := Dial(helper.MASTER_SERVER_PORT)
	defer client.Close()

	chunkRequest := models.ChunkLocationArgs{
		Filename:   filename,
		ChunkIndex: chunkIndex,
	}
	var reply models.ChunkMetadata
	client.Call("MasterNode.GetChunkLocation", chunkRequest, &reply)

	if chunkIndex != reply.LastIndex {
		log.Println("Last Index in arguments is incorrect LastIndex of chunkServer")
		log.Println("The last index of chunkServer is: ", reply.LastIndex)
	}

	return reply
}

// Read from a chunk in the chunk server
func ReadChunks(metadata models.ChunkMetadata, index1 int, index2 int) []byte {
	client := Dial(metadata.Location)
	defer client.Close()

	var reply []byte

	query := models.ReadData{
		ChunkIndex1:   index1,
		ChunkIndex2:   index2,
		ChunkMetadata: metadata,
	}

	err := client.Call("ChunkServer.ReadRange", query, &reply)
	if err != nil {
		log.Println("[Client] Error calling RPC method: ", err)
	}

	log.Println("Received chunk: ", string(reply))

	return reply
}

/* =============================== File-related functions =============================== */

func ReadFile(filename string, firstIndex int, LastIndex int) {
	// Compute number of chunks
	fi, err := os.Stat(filename)
	if err != nil {
		log.Println("[Client] Error acquiring file information: ", err)
	}
	chunks := ComputeNumberOfChunks(int(fi.Size()))

	// Read each chunk
	chunkMetadata := RequestChunkLocation(filename, chunks)
	ReadChunks(chunkMetadata, firstIndex, LastIndex)
	// TODO: update local copy here
}

// Append to a chunk in the chunk server
// TODO: can append at any point within the existing file
func AppendToFile(filename string, size int) {
	data := GenerateData(size)
	appendArgs := models.Append{Filename: filename, Data: data}
	var appendReply models.AppendData
	// var replicationReply models.ReplicationResponse

	// Sends a request to the master node. This request includes the file name it wants to append data to.
	mnClient := Dial(helper.MASTER_SERVER_PORT)
	err := mnClient.Call("MasterNode.Append", appendArgs, &appendReply)
	if err != nil {
		log.Println("[Client] Error calling RPC method: ", err)
	}
	mnClient.Close()

	// Append data to chunks
	var reply models.Chunk
	csClient := Dial(helper.CHUNK_SERVER_START_PORT)
	err = csClient.Call("ChunkServer.Append", appendReply, &reply)
	if err != nil {
		log.Println("[Client] Error calling RPC method: ", err)
	}
	csClient.Close()
	log.Println("[Client] Successfully appended payload: ", helper.TruncateOutput(data))

	// Append data to local copy of file
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("[Client] Error opening file: ", err)
	}
	_, err = f.Write(data)
	if err != nil {
		log.Println("[Client] Error writing to file: ", err)
	}
	err = f.Close()
	if err != nil {
		log.Println("[Client] Error closing file: ", err)
	}
}

func CreateFile(filename string, filesize int) {
	fmt.Println("CREATING FILE")

	// Create the file locally
	data := GenerateData(filesize)
	err := os.WriteFile(filename, data, 0644)
	if err != nil {
		log.Println("[Client] Error writing to file: ", err)
	}

	// Compute number of chunks and prepare args
	var metadata models.ChunkMetadata
	chunks := ComputeNumberOfChunks(filesize)
	createArgs := models.CreateData{
		Append:         models.Append{Filename: filename, Data: data},
		NumberOfChunks: chunks,
	}

	// Inform master server of new file
	mnClient := Dial(helper.MASTER_SERVER_PORT)
	err = mnClient.Call("MasterNode.CreateFile", createArgs, &metadata)
	if err != nil {
		log.Println("[Client] Error calling RPC method: ", err)
	}
	mnClient.Close()

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
	csClient := Dial(helper.CHUNK_SERVER_START_PORT)
	err = csClient.Call("ChunkServer.CreateFileChunks", chunkArray, &reply)
	if err != nil {
		log.Println("[Client] Error calling RPC method: ", err)
	}
	csClient.Close()

	log.Println("[Client] Successfully created file in chunkserver: ", reply)
}

/* =============================== Helper functions =============================== */

func Dial(address int) *rpc.Client {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(address))
	if err != nil {
		log.Println("[Client] Dialing error", err)
	}
	return client
}

// Creates a byte array with random characters
func GenerateData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = CHARACTERS[rand.Intn(len(CHARACTERS))]
	}
	return data
}

func ComputeNumberOfChunks(size int) int {
	chunks := size / helper.CHUNK_SIZE
	if size%helper.CHUNK_SIZE > 0 {
		chunks++
	}
	return chunks
}

/* =============================== Bootstrap functions =============================== */

// Start the client, to be called from main.go
func StartClients() {
	//runClient(Task{Operation: WRITE, Filename: FILE1, DataSize: 65536})
	runClient(Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
	runClient(Task{Operation: READ, Filename: FILE1})
	runClient(Task{Operation: APPEND, Filename: FILE1, DataSize: 10240})
}

func runClient(t Task) {
	if t.Operation == READ {
		ReadFile(t.Filename, 1, 3)
		return
	}

	if t.Operation == APPEND {
		AppendToFile(t.Filename, t.DataSize)
		return
	}

	if t.Operation == WRITE {
		CreateFile(t.Filename, t.DataSize)
		return
	}
}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("[Client] Error opening log file: ", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	rand.Seed(time.Now().UnixNano())
	StartClients()
}
