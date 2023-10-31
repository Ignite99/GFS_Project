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
}

/* =========== Replication & Append ===========*/
type SuccessJSON struct {
	FileID    uuid.UUID
	LastIndex int
}

type Append struct {
	Filename string
	Data     []int
}

type AppendData struct {
	ChunkMetadata
	Data []int
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
