package models

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type ChunkLocationArgs struct {
	Filename   string
	ChunkIndex int
}

type ChunkMetadata struct {
	Handle   uuid.UUID
	Location int
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

/* =========== Read ===========*/

/* =========== Read ===========*/

type ReadData struct {
	ChunkIndex    int
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

type Replication struct {
	Filename string
	Chunk    Chunk
}

type ReplicationResponse struct {
	FileName        string
	StorageLocation map[int]int
}

/* =========== Replication & Append ===========*/
