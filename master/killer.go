package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"

	"github.com/sutd_gfs_project/models"
)

// comment out to run this function
// func main() {
// 	killer(8092)
// }

// Testing get chunk location request
func killer(portNum int) {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(portNum)) // Replace with your master node's address
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer client.Close()

	args := 1
	var reply models.AckSigKill

	err = client.Call(strconv.Itoa(portNum)+".Kill", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}

	fmt.Printf("Killing for chunkserver at %s Ack: %v\n", strconv.Itoa(portNum), reply.Ack)
}
