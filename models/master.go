package models

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type GetChunkLocationArgs struct {
	Filename   string
	ChunkIndex int
}

type ChunkMetadata struct {
	Handle   uuid.UUID
	Location int
}

type ClientInfo struct {
	LastHeartbeat time.Time
	Status        string
	Mu            sync.Mutex
}
