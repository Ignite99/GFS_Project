package chunks

type ChunkMetadata struct {
	chunkHandle   int
	chunkLocation []byte
}

type ChunkServer struct {
	// assume we create a ChunkServer instance
	metadata ChunkMetadata
}

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

// command to run chunk server
func runChunkServer() {

}

// starting function for this file
func main() {

}
