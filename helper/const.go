package helper

import (
	"errors"

	"github.com/sutd_gfs_project/models"
)

const (
	BASE_URL = "http://localhost"

	MASTER_SERVER_PORT = 8080
	CLIENT_START_PORT  = 8081
)

var (
	Clients = make(map[string]*models.ClientInfo)

	ErrChunkNotFound   = errors.New("[ERROR] Chunk not found")
	ErrInvalidMetaData = errors.New("[ERROR] Invalid chunk metadata")
	ErrNotRegistered   = errors.New("[ERROR] Client not registered")
)
