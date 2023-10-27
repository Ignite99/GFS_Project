package main

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/sutd_gfs_project/models"
)

// Testing get chunk location request
func Test() {
	client, err := rpc.Dial("tcp", "localhost:8080") // Replace with your master node's address
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer client.Close()

	args := models.GetChunkLocationArgs{
		Filename:   "file1.txt",
		ChunkIndex: 1,
	}
	var reply models.ChunkMetadata

	err = client.Call("MasterNode.GetChunkLocation", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}

	fmt.Printf("Handle: %s, Location: %d\n", reply.Handle, reply.Location)
}
