package models

import (
	"time"

	"github.com/google/uuid"
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
}
