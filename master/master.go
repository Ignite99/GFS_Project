package main

import (
	"bufio"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"github.com/sutd_gfs_project/helper"
)

type MasterNode struct{}

var chunkServers map[string]bool

func (mn *MasterNode) CreateChunks(filePath string, reply *bool) error {
	// Open the input file for reading.
	inputFile, err := os.Open(filePath)
	if err != nil {
		log.Printf("Error opening input file: %v", err)
		return err
	}
	defer inputFile.Close()

	// Create a directory for storing chunks if it doesn't exist.
	err = os.MkdirAll("chunks", 0755)
	if err != nil {
		log.Printf("Error creating chunks directory: %v", err)
		return err
	}

	// Read the input file and create chunks.
	scanner := bufio.NewScanner(inputFile)
	chunkNumber := 1

	for scanner.Scan() {
		chunkFileName := "chunks/chunk" + strconv.Itoa(chunkNumber) + ".txt"
		chunkFile, err := os.Create(chunkFileName)
		if err != nil {
			log.Printf("Error creating chunk file: %v", err)
			return err
		}
		defer chunkFile.Close()

		// Write a line from the input file to the chunk file.
		chunkFile.WriteString(scanner.Text() + "\n")

		chunkNumber++
	}

	if scanner.Err() != nil {
		log.Printf("Error reading input file: %v", scanner.Err())
		return scanner.Err()
	}

	log.Printf("Created %d chunks from %s", chunkNumber-1, filePath)
	*reply = true
	return nil
}

func (mn *MasterNode) RegisterChunkServer(chunkServerAddress string, reply *bool) error {
	// Add the chunk server to the list of active servers.
	chunkServers[chunkServerAddress] = true
	*reply = true
	return nil
}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	myService := new(MasterNode)
	rpc.Register(myService)

	// Create a listener for the RPC server.
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}
	defer listener.Close()

	log.Printf("RPC server is listening on port %d", helper.MASTER_SERVER_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
