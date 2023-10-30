package models

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type GetChunkLocationArgs struct {
	Filename   string
	ChunkIndex int
}

type ChunkMetadata struct {
	Handle   uuid.UUID
	Location int //chunkserver location
}

type ChunkServerState struct {
	LastHeartbeat time.Time
	Status        string
	Node          int
}
