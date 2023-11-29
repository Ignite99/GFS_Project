package main

import (
	"log"
	"net/rpc"
	"os"
	"testing"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/client"
	"github.com/sutd_gfs_project/models"
)

// ** Master Functions **

func Test_CreateFile(t *testing.T){
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	data := []byte("New file!")
	err := os.WriteFile("testfile2.txt", data, 0644)
	if err != nil {
		log.Printf("[Master: CreateFile] Error writing to file: %v\n", err)
	}

	var metadata models.MetadataResponse
	chunks := len(data) / helper.CHUNK_SIZE
	if len(data)%helper.CHUNK_SIZE > 0 {
		chunks++
	}
	createArgs := models.CreateData{
		Append:         models.Append{Filename: "testfil2.txt", Data: data},
		NumberOfChunks: chunks,
	}

	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("[Master: CreateFile] Error connecting to RPC server:", err)
	}
	err = client.Call("MasterNode.CreateFile", createArgs, &metadata)
	if err != nil {
		log.Printf("[Master: CreateFile] Error calling RPC method: %v", err)
	}
	client.Close()
	log.Printf("[Master: CreateFile] Metadata of file created is received: %v\n", metadata)
}


// Testing get chunk location request
func Test_GetChunkLocation(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	newClient := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("New file!")
	newClient.CreateFile("testfile2.txt", data)
	client, err := rpc.Dial("tcp", "localhost:8080") // Replace with your master node's address
	if err != nil {
		log.Fatal("[Master: requestChunkLocation] Error connecting to RPC server:", err)
	}
	defer client.Close()

	args := models.ChunkLocationArgs{
		Filename:   "testfile2.txt",
		ChunkIndex: 1,
	}
	var reply models.MetadataResponse

	err = client.Call("MasterNode.GetChunkLocation", args, &reply)
	if err != nil {
		log.Fatal("[Master: requestChunkLocation] Error calling RPC method: ", err)
	}

	log.Printf("[Master: requestChunkLocation] Handle: %s, Location: %d\n", reply.Handle, reply.Location)
}

func Test_Replication(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	var successResponse models.SuccessJSON
	
	newChunk := models.Chunk {
		ChunkHandle: 	helper.StringToUUID("9e614b18-34f5-4773-86dc-21f920031c64"),
		ChunkIndex: 	25,
		Data:			[]byte("Morning"),
	}

	replicateChunk := models.Replication{
		Port: 8091,
		Chunk: newChunk,
	}

	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("[Master: Replication] Error connecting to RPC server:", err)
	}
	err = client.Call("MasterNode.Replication", replicateChunk, &successResponse)
	if err != nil {
		log.Printf("[Master: Replication] Error calling RPC method: %v\n", err)
	}

	log.Printf("[Master: Replication] Successful Replication: %v\n", successResponse)

	log.Println("[Master: Replication] Chunk added: ", newChunk.ChunkHandle, newChunk.ChunkIndex, newChunk.Data)

}

func Test_Append(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("[Master: Append] Error connecting to RPC server:", err)
	}
	appendArgs := models.Append{Filename: "testfile.txt", Data: []byte("New data")}
	var appendReply models.AppendData
	err = client.Call("MasterNode.Append", appendArgs, &appendReply)
	if err != nil {
		log.Printf("[Master: Append] Error calling RPC method: %v", err)
	}
	client.Close()
	log.Printf("[Master: Append] Metadata of file to append is received: %v\n", appendReply)
}

