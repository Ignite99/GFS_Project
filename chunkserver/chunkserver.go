package main

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"time"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

// data structure used for this GFS operation
// an Object is split into several Chunks
type Object []int

// each Chunk can be referred by its chunkHandle
type Chunk struct {
	chunkHandle int
	data        []int
}

// assume this is for a ChunkServer instance
type ChunkServer struct {
	// assume everything stored in database within ChunkServer
	database []Chunk
}

/* =============================== Chunk functions =============================== */

// get chunk from database
func (cs *ChunkServer) getChunk(chunkHandle int) Chunk {
	var chunkRetrieved Chunk
	// loop through database of ChunkServer
	for _, val := range cs.database {
		// find chunk in database with the same chunkHandle
		if val.chunkHandle == chunkHandle {
			chunkRetrieved = val
			break
		}
	}
	return chunkRetrieved
}

// add new chunk to database
func (cs *ChunkServer) addChunk(newChunk Chunk) Chunk {
	cs.database = append(cs.database, newChunk)
	return newChunk
}

// update value of chunk in database
func (cs *ChunkServer) updateChunk(chunk Chunk) Chunk {
	var updatedChunk Chunk
	for idx, val := range cs.database {
		if val.chunkHandle == chunk.chunkHandle {
			cs.database[idx] = chunk
			updatedChunk = cs.database[idx]
			break
		}
	}
	return updatedChunk
}

// delete chunk from database
func (cs *ChunkServer) deleteChunk(chunk Chunk) Chunk {
	var deletedChunk Chunk
	for idx, val := range cs.database {
		if val.chunkHandle == chunk.chunkHandle {
			cs.database = append(cs.database[:idx], cs.database[:idx+1]...)
			deletedChunk = chunk
			break
		}
	}
	return deletedChunk
}

/* ============================ ChunkServer functions ============================ */

// chunk server to reply heartbeat to master to verify that it is alive
func (cs *ChunkServer) SendHeartBeat(args models.ChunkServerInfo, reply *models.ChunkServerInfo) error {
	heartBeat := models.ChunkServerInfo{
		LastHeartbeat: time.Now(),
		Status:        args.Status,
	}
	*reply = heartBeat
	return nil
}

// client to call this API when it wants to read data
func (cs *ChunkServer) read(chunkHandle int, reply *Chunk) error {
	// will add more logic here
	return nil
}

// client to call this API when it wants to append data
func (cs *ChunkServer) append(chunkHandle int, reply *Chunk) error {
	// will add more logic here
	return nil
}

// client to call this API when it wants to truncate data
func (cs *ChunkServer) truncate(chunkHandle int, reply *Chunk) error {
	// will add more logic here
	return nil
}

// master to call this when it needs to create new replica for a chunk
func (cs *ChunkServer) createNewReplica() {

}

// master to call this when it needs to remove a chunk
func (cs *ChunkServer) removeChunk() {

}

// master to call this to pass lease to chunk server
func (cs *ChunkServer) receiveLease() {

}

// start RPC server for chunk server (refer to Go's RPC documentation for more details)
func startRPCServer() {
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

// function to run a ChunkServer instance
func (cs *ChunkServer) run() {

}

// command or API call for MAIN function to run chunk server
func runChunkServer() {
	chunkServerInstance := new(ChunkServer)
	rpc.Register(chunkServerInstance)
	startRPCServer()
	chunkServerInstance.run()
}

// starting function for this file --> will be moved to main.go
func main() {
	runChunkServer()
}
