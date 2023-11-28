package main

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/sutd_gfs_project/models"
)

// Testing get chunk location request
func main() {
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
