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
	Handle    uuid.UUID
	Location  []int
	LastIndex int
}

type MetadataResponse struct {
	Handle    uuid.UUID
	Location  int
	LastIndex int
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
	ChunkMetadata MetadataResponse
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
	MetadataResponse MetadataResponse
	Data             []byte
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

/* =========== Leases ===========*/
const LeaseDuration = time.Second * 5 // in seconds

type Lease struct {
	ChunkHandle uuid.UUID
	Owner       int // port number of chunk server
	Expiration  time.Time
	IsExpired   bool
}

type LeaseData struct {
	FileID   string // can be any file identifier by right
	Owner    int
	Duration time.Duration
}
