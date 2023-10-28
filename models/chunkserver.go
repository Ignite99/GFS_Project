package models

import (
	"github.com/google/uuid"
)

type ChunkLocation struct {
	Handle uuid.UUID
	Offset int64
}
