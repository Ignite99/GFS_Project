package main

import (
	"fmt"
	"log"
	"net/rpc"
)

func Tester() {
	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	defer client.Close()

	// Path to your example.txt file.
	filePath := "example.txt"

	var reply bool
	err = client.Call("MasterNode.CreateChunks", filePath, &reply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}

	if reply {
		fmt.Println("CreateChunks RPC method successfully executed.")
	} else {
		fmt.Println("CreateChunks RPC method failed.")
	}
}
