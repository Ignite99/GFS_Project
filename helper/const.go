package helper

import (
	"errors"

	"github.com/sutd_gfs_project/models"
)

const (
	BASE_URL = "http://localhost"

	MASTER_SERVER_PORT      = 8080
	CLIENT_START_PORT       = 8081
	CHUNK_SERVER_START_PORT = 8090
)

var (
	ChunkServers = make(map[string]*models.ChunkServerState)

	ErrChunkNotFound   = errors.New("[ERROR] Chunk not found")
	ErrInvalidMetaData = errors.New("[ERROR] Invalid chunk metadata")
	ErrNotRegistered   = errors.New("[ERROR] Client not registered")
)
