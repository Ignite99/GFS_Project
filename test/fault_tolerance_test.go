package main

import (
	"log"
	"net/rpc"
	"os"
	"testing"

	"github.com/sutd_gfs_project/client"
	"github.com/sutd_gfs_project/models"
)

// Must run master.go first for these tests to work!
// Test if operations still work after killing chunk server

type Task struct {
	Operation int
	Filename  string
	DataSize  int
}

type Client struct {
	ID int
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

func TestOperationsAfterKillingChunkServer(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	log.Println("[System: OpsAfterKillingChunkServer] We will be killing one chunkserver for this test")
	c, err := rpc.Dial("tcp", "localhost:8090") // Replace with your master node's address
	if err != nil {
		log.Fatal("[System: OpsAfterKillingChunkServer] Error connecting to RPC server:", err)
	}
	defer c.Close()

	args := 1
	var reply models.AckSigKill

	err = c.Call("8090.Kill", args, &reply)
	if err != nil {
		log.Fatal("[System: OpsAfterKillingChunkServer] Error calling RPC method: ", err)
	}

	log.Printf("[System: OpsAfterKillingChunkServer] Killing for chunkserver at 8090 Ack: %v\n", reply.Ack)

	log.Printf("[System: OpsAfterKillingChunkServer] Running Write Test after Killing Chunk Server..")
	newClient1 := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data1 := []byte("This the write test.")
	newClient1.CreateFile("testfile1.txt", data1)

	log.Printf("[System: OpsAfterKillingChunkServer] Running Read Test after Killing Chunk Server....")
	newClient2 := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	newClient2.ReadFile("testfile1.txt", 0, 0)

	log.Printf("[System: OpsAfterKillingChunkServer] Running Append Test after Killing Chunk Server....")
	newClient3 := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data2 := []byte("This is the append test.")
	newClient3.AppendToFile("testfile1.txt", data2)

}
