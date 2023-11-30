package main

import (
	"fmt"
	"log"
	"net/rpc"
	"testing"

	"github.com/sutd_gfs_project/client"
	"github.com/sutd_gfs_project/models"
)

// Must run master.go first for these tests to work!
// Test Writing File
func TestWrite(t *testing.T) {
	fmt.Printf("Running Write Test..")
	newClient := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("This the write test.")
	newClient.CreateFile("testfile.txt", data)
}

// Test Reading File from 1 chunk server
func TestRead(t *testing.T) {
	fmt.Printf("Running Read Test..")
	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	newClient.ReadFile("testfile.txt", 0, 0)

}

// Test Reading File from another chunk server
// Need to hardcode a chunk server to read from, but how?
// func TestReadReplica(t *testing.T) {
// 	fmt.Printf("Running Read Test..")
// 	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}

// }

// Test Appending Content
func TestAppend(t *testing.T) {
	fmt.Printf("Running Append Test..")
	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("This is the append test.")
	newClient.AppendToFile("testfile.txt", data)
}

func TestKillChunkServer(t *testing.T) {
	fmt.Println("We will be killing one chunkserver for this test")
	client, err := rpc.Dial("tcp", "localhost:8090") // Replace with your master node's address
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer client.Close()

	args := 1
	var reply models.AckSigKill

	err = client.Call("8090.Kill", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}

	fmt.Printf("Killing for chunkserver at 8090 Ack: %v\n", reply.Ack)

}
