package chunkserver

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type ChunkMetadata struct {
	chunkHandle   int
	chunkLocation []byte
}

type Chunk struct {
	// assume this is the data structure for chunks
	metadata ChunkMetadata
	data     string
}

// assume this is for a ChunkServer instance
type ChunkServer struct {
	// assume everything stored in database within ChunkServer
	database []Chunk
}

/* ---------------------------------Chunk functions-------------------------------- */

// get chunk from database
func (cs *ChunkServer) getChunk(chunkHandle int) Chunk {
	var chunkRetrieved Chunk
	// loop through database of ChunkServer
	for _, val := range cs.database {
		// find chunk in database with the same chunkHandle
		if val.metadata.chunkHandle == chunkHandle {
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
		if val.metadata.chunkHandle == chunk.metadata.chunkHandle {
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
		if val.metadata.chunkHandle == chunk.metadata.chunkHandle {
			cs.database = append(cs.database[:idx], cs.database[:idx+1]...)
			deletedChunk = chunk
			break
		}
	}
	return deletedChunk
}

/* -----------------------------ChunkServer functions------------------------------ */

// chunk server to send heartbeat to master to check if its alive
func (cs *ChunkServer) sendHeartBeat() {

}

// client to call this when it wants to write data
func (cs *ChunkServer) writeData() {

}

// client to call this when it wants to read data
func (cs *ChunkServer) readData() {

}

// client to call this when it wants to append data
func (cs *ChunkServer) append() {

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
func startRPC() {
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", "localhost:8080")

	if err != nil {
		log.Fatal("Listener error", err)
	}

	log.Printf("Serving RPC on port %d", 8080)
	err = http.Serve(listener, nil)

	if err != nil {
		log.Fatal("Error serving", err)
	}
}

// command to run chunk server
func runChunkServer() {
	startRPC()
}

// starting function for this file
func main() {
	runChunkServer()
}
