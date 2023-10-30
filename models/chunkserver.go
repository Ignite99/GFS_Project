package models

import uuid "github.com/satori/go.uuid"

// each Chunk can be referred by its chunkHandle
type Chunk struct {
	ChunkHandle uuid.UUID
	Data        []int
