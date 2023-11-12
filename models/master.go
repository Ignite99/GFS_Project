package models

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type Lease struct {
	Expiry	time.Time
	Handle	uuid.UUID
}

type ChunkLocationArgs struct {
	Filename   string
	ChunkIndex int
}

type ChunkMetadata struct {
	Handle    	uuid.UUID
	Location  	int
	LastIndex 	int
	Lease		*Lease
}

type ChunkServerState struct {
	LastHeartbeat time.Time
	Status        string
	Node          int
	Port          int
}

/* =========== CreateFile ===========*/

type CreateData struct {
	Append         Append
	NumberOfChunks int
}

/* =========== CreateFile ===========*/

/* =========== Read ===========*/

type ReadData struct {
	ChunkIndex1   int
	ChunkIndex2   int
	ChunkMetadata ChunkMetadata
}

/* =========== Read ===========*/

/* =========== Replication & Append ===========*/
type SuccessJSON struct {
	FileID    uuid.UUID
	LastIndex int
}

type Append struct {
	Filename string
	Data     []byte
}

type AppendData struct {
	ChunkMetadata
	Data []byte
}

type ReleaseLeaseData struct {
	ChunkMetadata
	Chunk
}

type Replication struct {
	Port  int
	Chunk Chunk
}

type ReplicationResponse struct {
	FileName        string
	StorageLocation map[int]int
}

/* =========== Replication & Append ===========*/
