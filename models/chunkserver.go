package models

import uuid "github.com/satori/go.uuid"

// each Chunk can be referred by its chunkHandle
type Chunk struct {
	ChunkHandle uuid.UUID
	Filename    string
	ChunkIndex  int
	Data        []byte
}

type ReadDataStream struct {
	ChunkID uuid.UUID
	Data    []int
}

type AckSigKill struct {
	Ack bool
}
