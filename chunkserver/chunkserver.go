package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

type ChunkServer struct {
	storage []models.Chunk
	portNum int
}

/* =============================== Chunk Storage functions =============================== */
// Master will talk with these Chunk functions to create, update, delete, or replicate chunks

// get chunk from storage
func (cs *ChunkServer) GetChunk(chunkHandle uuid.UUID, chunkLocation int) models.Chunk {
	var chunkRetrieved models.Chunk

	// Only iterate through those with relevant uuid. Will only enter upon detecting ChunkLocation is similar to chunkIndex
	for _, val := range cs.storage {
		if val.ChunkHandle == chunkHandle && val.ChunkIndex == chunkLocation {
			chunkRetrieved = val
			break
		}
	}

	return chunkRetrieved
}

// add new chunk to storage
func (cs *ChunkServer) AddChunk(args models.Chunk, reply *models.SuccessJSON) error {
	log.Println("============== ADDING CHUNK ==============")
	//log.Println("Chunk added: ", args)
	log.Println("[ChunkServer] Chunk added: ", fmt.Sprintf("{%v %d %s}", args.ChunkHandle, args.ChunkIndex, helper.TruncateOutput(args.Data)))
	cs.storage = append(cs.storage, args)
	index := len(cs.storage)

	*reply = models.SuccessJSON{
		FileID:    args.ChunkHandle,
		LastIndex: index,
	}
	return nil
}

func (cs *ChunkServer) CreateFileChunks(args []models.Chunk, reply *models.SuccessJSON) error {
	log.Println("============== CREATE CHUNKS IN CHUNK SERVER ==============")
	//log.Println("Chunk added: ", args)
	logMessage := "[ChunkServer] Chunks added: "

	for _, c := range args {
		cs.storage = append(cs.storage, c)
		logMessage += fmt.Sprintf("\n{%v %d %s}", c.ChunkHandle, c.ChunkIndex, helper.TruncateOutput(c.Data))
	}
	log.Println(logMessage)

	index := len(cs.storage)

	*reply = models.SuccessJSON{
		FileID:    args[0].ChunkHandle,
		LastIndex: index,
	}
	return nil
}

// update value of chunk in storage
func (cs *ChunkServer) UpdateChunk(args models.Chunk, reply *models.Chunk) error {
	var updatedChunk models.Chunk
	for idx, val := range cs.storage {
		if val.ChunkHandle == args.ChunkHandle {
			cs.storage[idx] = args
			updatedChunk = cs.storage[idx]
			break
		}
	}
	*reply = updatedChunk
	return nil
}

// delete chunk from storage
func (cs *ChunkServer) DeleteChunk(args models.Chunk, reply *models.Chunk) error {
	var deletedChunk models.Chunk
	for idx, val := range cs.storage {
		if val.ChunkHandle == args.ChunkHandle {
			cs.storage = append(cs.storage[:idx], cs.storage[:idx+1]...)
			deletedChunk = args
			break
		}
	}
	*reply = deletedChunk
	return nil
}

/* ============================ ChunkServer functions ============================ */

// chunk server to reply heartbeat to master to verify that it is alive
func (cs *ChunkServer) SendHeartBeat(args models.ChunkServerState, reply *models.ChunkServerState) error {
	heartBeat := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        args.Status,
		Node:          helper.CHUNK_SERVER_START_PORT,
		Port:          cs.portNum,
	}
	*reply = heartBeat
	return nil
}

// client to call this API when it wants to read data
func (cs *ChunkServer) ReadRange(args models.ReadData, reply *[]byte) error {
	var dataStream []byte

	chunkUUID := args.ChunkMetadata.Handle

	if args.ChunkIndex1 == args.ChunkIndex2 {
		for _, chunk := range cs.storage {
			if chunk.ChunkHandle == chunkUUID && chunk.ChunkIndex == args.ChunkIndex1 {
				dataStream = append(dataStream, chunk.Data...)
			}
		}
	} else {
		for _, chunk := range cs.storage {
			if chunk.ChunkHandle == chunkUUID && chunk.ChunkIndex >= args.ChunkIndex1 && chunk.ChunkIndex <= args.ChunkIndex2 {
				dataStream = append(dataStream, chunk.Data...)
			}
		}
	}

	*reply = dataStream

	return nil
}

// client to call this API when it wants to append data
func (cs *ChunkServer) Append(args models.AppendData, reply *models.Chunk) error {
	var successResponse models.SuccessJSON
	chunk := cs.GetChunk(args.ChunkMetadata.Handle, args.ChunkMetadata.LastIndex)
	index := chunk.ChunkIndex
	chunkSpace := helper.CHUNK_SIZE - len(chunk.Data)

	// Make new chunks while data size is greater than chunk size
	for len(args.Data) > chunkSpace {
		// Append as much data to last chunk as possible
		chunk.Data = append(chunk.Data, args.Data[:chunkSpace]...)
		args.Data = args.Data[chunkSpace:]
		chunkSpace = helper.CHUNK_SIZE

		// Create new chunk for next iteration
		index++
		chunk = models.Chunk{ChunkHandle: args.Handle, ChunkIndex: index, Data: []byte{}}

		// Updates Master for new last index entry
		client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
		if err != nil {
			log.Println("[ChunkServer] Dialing error: ", err)
		}

		err = client.Call("MasterNode.Replication", chunk, &successResponse)
		if err != nil {
			log.Println("[ChunkServer] Error calling RPC method: ", err)
		}
		client.Close()

		log.Println("[ChunkServer] Successful Replication: ", successResponse)
	}

	// Append remaining data to last chunk
	chunk.Data = append(chunk.Data, args.Data...)

	*reply = chunk
	return nil
}

// client to call this API when it wants to truncate data
// func (cs *ChunkServer) Truncate(chunkMetadata models.ChunkMetadata, reply *models.Chunk) error {
// 	// will add more logic here
// 	chunkToTruncate := cs.GetChunk(chunkMetadata.Handle)
// 	cs.DeleteChunk(chunkToTruncate, nil)

// 	*reply = chunkToTruncate
// 	return nil
// }

// master to call this when it needs to create new replica for a chunk
// func (cs *ChunkServer) CreateNewReplica() {
// 	chunkServerReplica := new(ChunkServer)
// 	rpc.Register(chunkServerReplica)
// 	chunkServerReplica.storage = cs.storage
// }

// master to call this to pass lease to chunk server
func (cs *ChunkServer) ReceiveLease() {

}

// command or API call for MAIN function to run chunk server
func runChunkServer(portNumber int) {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("[ChunkServer] Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	// initialize chunk server instance
	chunkServerInstance := &ChunkServer{
		storage: make([]models.Chunk, 0),
		portNum: portNumber,
	}
	rpc.Register(chunkServerInstance)

	chunkServerInstance.InitialiseChunks()
	chunkServerInstance.Registration(portNumber)

	// start RPC server for chunk server (refer to Go's RPC documentation for more details)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(portNumber))

	if err != nil {
		log.Fatal("[ChunkServer] Error starting RPC server", err)
	}
	defer listener.Close()

	log.Printf("[ChunkServer] RPC listening on port %d", portNumber)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("[ChunkServer] Error accepting connection", err)
		}
		// serve incoming RPC requests
		go rpc.ServeConn(conn)
	}
}

// starting function for this file --> will be moved to main.go
func main() {
	var portNumber int

	flag.IntVar(&portNumber, "portNumber", helper.CHUNK_SERVER_START_PORT, "Port number of Chunk Server.")
	flag.Parse()

	// go run chunkserver.go --portNumber 8090-8095
	// Port number arguments

	runChunkServer(portNumber)
}

func (cs *ChunkServer) InitialiseChunks() {
	fmt.Println("Initialised chunkServer Data")
	uuid1 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")

	chunk1 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  0,
		Data:        []byte("Hello"),
	}
	chunk2 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  1,
		Data:        []byte("World"),
	}
	chunk3 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  2,
		Data:        []byte("Foo"),
	}
	chunk4 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  3,
		Data:        []byte("Bar"),
	}

	cs.storage = append(cs.storage, chunk1, chunk2, chunk3, chunk4)
}

func (cs *ChunkServer) Registration(portNum int) {
	var response string

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Println("[ChunkServer] Dialing error: ", err)
	}

	err = client.Call("MasterNode.RegisterChunkServers", portNum, &response)
	if err != nil {
		log.Println("[ChunkServer] Error calling RPC method: ", err)
	}
	client.Close()

	log.Printf("[ChunkServer] ChunkServer on port: %d. Registration Response %s\n", portNum, response)

}
