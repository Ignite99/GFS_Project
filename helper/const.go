package helper

import (
	"errors"
	"sync"

	"github.com/sutd_gfs_project/models"
)

const (
	BASE_URL = "http://localhost"

	MASTER_SERVER_PORT      = 8080
	CLIENT_START_PORT       = 8081
	CHUNK_SERVER_START_PORT = 8090

	CHUNK_SIZE = 65536 // 64kB
)

var (
	ChunkServers     = make(map[string]*models.ChunkServerState)
	AckMap           sync.Map
	ChunkServerPorts []int

	ErrChunkNotFound   = errors.New("[ERROR] Chunk not found")
	ErrInvalidMetaData = errors.New("[ERROR] Invalid chunk metadata")
	ErrNotRegistered   = errors.New("[ERROR] Client not registered")
)
