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

	CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
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
	return reply
}

// Read from a chunk in the chunk server
func ReadChunk(metadata models.ChunkMetadata) []byte {
	client := Dial(helper.CHUNK_SERVER_START_PORT)
	defer client.Close()

	var reply models.Chunk
	err := client.Call("ChunkServer.Read", metadata, &reply)
	if err != nil {
		log.Println("Error calling RPC method: ", err)
	}
	fmt.Println("Received chunk: ", string(reply.Data))

	return reply.Data
}

// Append to a chunk in the chunk server
func AppendChunk(filename string, data []byte) {
	var appendReply models.AppendData
	var reply models.Chunk
	// var replicationReply models.ReplicationResponse

	client1 := Dial(helper.MASTER_SERVER_PORT)
	appendArgs := models.Append{Filename: filename, Data: data}

	// Sends a request to the master node. This request includes the file name it wants to append data to.
	err := client1.Call("MasterNode.Append", appendArgs, &appendReply)
	if err != nil {
		log.Println("Error calling RPC method: ", err)
	}
	client1.Close()

	client2 := Dial(helper.CHUNK_SERVER_START_PORT)
	err = client2.Call("ChunkServer.Append", appendReply, &reply)
	if err != nil {
		log.Println("Error calling RPC method: ", err)
	}
	client2.Close()

	log.Println("Successfully appended payload: ", string(reply.Data))
}

/* =============================== File-related functions =============================== */

func ReadFile(filename string) {
	// Compute number of chunks
	// fi, err := os.Stat(filename)
	// if err != nil {
	// 	log.Println("Error acquiring file information: ", err)
	// }
	// chunks := fi.Size()/helper.CHUNK_SIZE + 1

	i := 2

	// Read each chunk
	chunkMetadata := RequestChunkLocation(filename, i)
	ReadChunk(chunkMetadata)

}

func AppendToFile(filename string, size int) {
	data := GenerateData(size)
	AppendChunk(filename, data)
	// TODO: append to local copy of file here if ok response
}

func CreateFile(filename string, filesize int) {
	var reply models.ChunkMetadata
	var reply2 models.SuccessJSON
	var chunkArray []models.Chunk
	var dataSplitted []byte

	chunks := filesize/helper.CHUNK_SIZE + 1

	fmt.Println("CREATING FILE")
	// Create the file locally
	data := GenerateData(filesize)
	err := os.WriteFile(filename, data, 0666)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	createFileMetadata := models.Append{
		Filename: filename,
		Data:     data,
	}

	createArgs := models.CreateData{
		Append:         createFileMetadata,
		NumberOfChunks: chunks,
	}

	// Push to master server
	client1 := Dial(helper.MASTER_SERVER_PORT)
	err = client1.Call("MasterNode.CreateFile", createArgs, &reply)
	if err != nil {
		log.Println("Error calling RPC method: ", err)
	}
	client1.Close()

	// Reply has the chunk uuid and location(last index)

	for i := 0; i < chunks; i++ {
		chunked := models.Chunk{
			ChunkHandle: reply.Handle,
			ChunkIndex:  i,
			Data:        dataSplitted,
		}

		chunkArray = append(chunkArray, chunked)
	}

	client2 := Dial(helper.CHUNK_SERVER_START_PORT)
	err = client2.Call("ChunkServer.CreateFileChunks", chunkArray, &reply2)
	if err != nil {
		log.Println("Error calling RPC method: ", err)
	}
	client2.Close()

	log.Println("Successful in creating file in chunkserver: ", reply2)
}

/* =============================== Helper functions =============================== */

func Dial(address int) *rpc.Client {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(address))
	if err != nil {
		log.Println("Dialing error", err)
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

/* =============================== Bootstrap functions =============================== */

// Start the client, to be called from main.go
func StartClients() {
	runClient(Task{Operation: WRITE, Filename: FILE1, DataSize: 64})
	//runClient(Task{Operation: WRITE, Filename: FILE3, DataSize: 65})
	runClient(Task{Operation: READ, Filename: FILE1})
	runClient(Task{Operation: APPEND, Filename: FILE1, DataSize: 10})
}

func runClient(t Task) {
	if t.Operation == READ {
		ReadFile(t.Filename)
		return
	}

	if t.Operation == APPEND {
		AppendToFile(t.Filename, t.DataSize)
		return
	}

	if t.Operation == WRITE {
		CreateFile(t.Filename, t.DataSize)
	}
}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	rand.Seed(time.Now().UnixNano())
	StartClients()
}
