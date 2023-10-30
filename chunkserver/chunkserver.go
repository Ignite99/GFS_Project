package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

// data structure used for this GFS operation
// an Object is split into several Chunks
type Object []int

// each Chunk can be referred by its chunkHandle
type Chunk struct {
	chunkHandle uuid.UUID
	data        []int
}

// assume this is for a ChunkServer instance
type ChunkServer struct {
	// assume everything stored in storage within ChunkServer
	Location        int // same as location in chunkmetadata
	storage         []Chunk
	ChunkingStorage sync.Map
}

/* =============================== Chunk Storage functions =============================== */
// Master will talk with these Chunk functions to create, update, delete, or replicate chunks

// get chunk from storage
func (cs *ChunkServer) getChunk(chunkHandle uuid.UUID) Chunk {
	var chunkRetrieved Chunk
	// loop through database of ChunkServer
	for _, val := range cs.storage {
		// find chunk in database with the same chunkHandle
		if val.chunkHandle == chunkHandle {
			chunkRetrieved = val
			break
		}
	}
	return chunkRetrieved
}

// add new chunk to storage
func (cs *ChunkServer) addChunk(args Chunk, reply *Chunk) error {
	cs.storage = append(cs.storage, args)
	*reply = args
	return nil
}

// update value of chunk in storage
func (cs *ChunkServer) updateChunk(args Chunk, reply *Chunk) error {
	var updatedChunk Chunk
	for idx, val := range cs.storage {
		if val.chunkHandle == args.chunkHandle {
			cs.storage[idx] = args
			updatedChunk = cs.storage[idx]
			break
		}
	}
	*reply = updatedChunk
	return nil
}

// delete chunk from storage
func (cs *ChunkServer) deleteChunk(args Chunk, reply *Chunk) error {
	var deletedChunk Chunk
	for idx, val := range cs.storage {
		if val.chunkHandle == args.chunkHandle {
			cs.storage = append(cs.storage[:idx], cs.storage[:idx+1]...)
			deletedChunk = args
			break
		}
	}
	*reply = deletedChunk
	return nil
}

/* ============================ ChunkServer functions ============================ */

// chunk server to reply heartbeat to master to verify that it is alive
func (cs *ChunkServer) SendHeartBeat(args models.ChunkServerState, reply *models.ChunkServerState) error {
	heartBeat := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        args.Status,
		Node:          helper.CHUNK_SERVER_START_PORT,
	}
	*reply = heartBeat
	return nil
}

// client to call this API when it wants to read data
func (cs *ChunkServer) Read(chunkMetadata models.ChunkMetadata, reply *Chunk) error {
	// will add more logic here
	ch := chunkMetadata.Handle
	for _, chunk := range cs.storage {
		if chunk.chunkHandle == ch {
			fmt.Println(chunk.data)
			*reply = chunk
		}
	}

	return nil
}

// client to call this API when it wants to append data
func (cs *ChunkServer) Append(chunkMetadata models.ChunkMetadata, reply *Chunk, data []int) error {

	chunktoappend := cs.getChunk(chunkMetadata.Handle)
	chunktoappend.data = append(chunktoappend.data, data...)

	cs.addChunk(chunktoappend, nil)

	*reply = chunktoappend
	return nil
}

// client to call this API when it wants to truncate data
func (cs *ChunkServer) Truncate(chunkMetadata models.ChunkMetadata, reply *Chunk) error {
	// will add more logic here
	chunkToTruncate := cs.getChunk(chunkMetadata.Handle)
	cs.deleteChunk(chunkToTruncate, nil)

	*reply = chunkToTruncate
	return nil
}

// master to call this when it needs to create new replica for a chunk
func (cs *ChunkServer) createNewReplica() {
	chunkServerReplica := new(ChunkServer)
	rpc.Register(chunkServerReplica)
	chunkServerReplica.storage = cs.storage

}

// master to call this to pass lease to chunk server
func (cs *ChunkServer) receiveLease() {

}

// command or API call for MAIN function to run chunk server
func runChunkServer() {
	chunkServerInstance := new(ChunkServer)
	rpc.Register(chunkServerInstance)

	// start RPC server for chunk server (refer to Go's RPC documentation for more details)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.CHUNK_SERVER_START_PORT))

	if err != nil {
		log.Fatal("Error starting RPC server", err)
	}
	defer listener.Close()

	log.Printf("RPC listening on port %d", helper.CHUNK_SERVER_START_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Error accepting connection", err)
		}
		// serve incoming RPC requests
		go rpc.ServeConn(conn)
	}
}

// starting function for this file --> will be moved to main.go
func main() {
	runChunkServer()
}
