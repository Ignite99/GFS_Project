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
// Test if operations still work after killing chunk server
func TestOperationsAfterKillingChunkServer(t *testing.T) {
	fmt.Println("We will be killing one chunkserver for this test")
	c, err := rpc.Dial("tcp", "localhost:8090") // Replace with your master node's address
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer c.Close()

	args := 1
	var reply models.AckSigKill

	err = c.Call("8090.Kill", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}

	fmt.Printf("Killing for chunkserver at 8090 Ack: %v\n", reply.Ack)

	fmt.Printf("Running Write Test after Killing Chunk Server..")
	newClient1 := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data1 := []byte("This the write test.")
	newClient1.CreateFile("testfile1.txt", data1)

	fmt.Printf("Running Read Test after Killing Chunk Server....")
	newClient2 := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	newClient2.ReadFile("testfile1.txt", 0, 0)

	fmt.Printf("Running Append Test after Killing Chunk Server....")
	newClient3 := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data2 := []byte("This is the append test.")
	newClient3.AppendToFile("testfile1.txt", data2)

}
