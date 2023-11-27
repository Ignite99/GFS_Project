package chunkserver

import (
	//"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"

	//"os"
	"strconv"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

//var portNumber int

type ChunkServer struct {
	storage []models.Chunk
	portNum int
	killed  bool
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
	log.Printf("[ChunkServer %d] Chunk added: {%v %d %s}\n", cs.portNum, args.ChunkHandle, args.ChunkIndex, helper.TruncateOutput(args.Data))
	cs.storage = append(cs.storage, args)
	index := len(cs.storage)

	*reply = models.SuccessJSON{
		FileID:    args.ChunkHandle,
		LastIndex: index,
	}
	return nil
}

func (cs *ChunkServer) CreateFileChunks(args []models.Chunk, reply *models.SuccessJSON) error {
	var successResponse models.SuccessJSON

	log.Println("============== CREATE CHUNKS IN CHUNK SERVER ==============")
	//log.Println("Chunk added: ", args)
	logMessage := fmt.Sprintf("[ChunkServer %d] Chunks added: ", cs.portNum)

	for _, c := range args {
		cs.storage = append(cs.storage, c)

		newChunk := models.Chunk{
			ChunkHandle: c.ChunkHandle,
			ChunkIndex:  c.ChunkIndex,
			Data:        c.Data,
		}

		replicateChunk := models.Replication{
			Port:  cs.portNum,
			Chunk: newChunk,
		}

		client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
		if err != nil {
			log.Printf("[ChunkServer %d] Dialing error: %v\n", cs.portNum, err)
		}

		err = client.Call("MasterNode.Replication", replicateChunk, &successResponse)
		if err != nil {
			log.Printf("[ChunkServer %d] Error calling RPC method: %v\n", cs.portNum, err)
		}
		client.Close()

		log.Printf("[ChunkServer %d] Successful Replication: %v\n", cs.portNum, successResponse)

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

	fmt.Println(args)

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

	// Append as much as possible to last chunk
	if len(args.Data) <= chunkSpace {
		chunk.Data = append(chunk.Data, args.Data...)
	} else {
		chunk.Data = append(chunk.Data, args.Data[:chunkSpace]...)
		args.Data = args.Data[chunkSpace:]
		chunkSpace = helper.CHUNK_SIZE

		// Make new chunks until all data is appended
		for len(args.Data) > 0 {
			index++
			chunk = models.Chunk{ChunkHandle: args.ChunkMetadata.Handle, ChunkIndex: index, Data: []byte{}}
			if len(args.Data) <= chunkSpace {
				chunk.Data = append(chunk.Data, args.Data...)
				args.Data = nil
			} else {
				chunk.Data = append(chunk.Data, args.Data[:chunkSpace]...)
				args.Data = args.Data[chunkSpace:]
			}

			replicateChunk := models.Replication{Port: cs.portNum, Chunk: chunk}

			// Updates Master for new last index entry
			client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
			if err != nil {
				log.Printf("[ChunkServer %d] Dialing error: %v\n", cs.portNum, err)
			}

			err = client.Call("MasterNode.Replication", replicateChunk, &successResponse)
			if err != nil {
				log.Printf("[ChunkServer %d] Error calling RPC method: %v\n", cs.portNum, err)
			}
			client.Close()

			log.Printf("[ChunkServer %d] Successful Replication: %v\n", cs.portNum, successResponse)
		}
	}

	*reply = chunk
	return nil
}

func (cs *ChunkServer) Kill(args int, reply *models.AckSigKill) error {
	cs.killed = true
	*reply = models.AckSigKill{Ack: true}
	log.Printf("[ChunkServer %d] Received kill signal\n", cs.portNum)
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

// command or API call for MAIN function to run chunk server
func RunChunkServer(portNumber int) {
	/*
		logfile, err := os.OpenFile("../logs/chunkServer_"+strconv.Itoa(portNumber)+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatal("[ChunkServer] Error opening log file:", err)
		}
		defer logfile.Close()
		log.SetOutput(logfile)
	*/

	// initialize chunk server instance
	chunkServerInstance := &ChunkServer{
		storage: make([]models.Chunk, 0),
		portNum: portNumber,
	}
	/*
		err = rpc.Register(chunkServerInstance)
		if err != nil {
			fmt.Println(err)
		}
	*/
	err := rpc.RegisterName(fmt.Sprintf("%d", portNumber), chunkServerInstance)
	if err != nil {
		log.Fatalf("[ChunkServer %d] Failed to register to RPC: %v\n", portNumber, err)
	}

	chunkServerInstance.InitialiseChunks()
	chunkServerInstance.Registration(portNumber)

	// start RPC server for chunk server (refer to Go's RPC documentation for more details)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(portNumber))

	if err != nil {
		log.Fatalf("[ChunkServer %d] Error starting RPC server: %v\n", portNumber, err)
	}
	defer listener.Close()

	log.Printf("[ChunkServer %d] RPC listening on port %d\n", portNumber, portNumber)

	for {
		if chunkServerInstance.killed {
			return
		}
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("[ChunkServer %d] Error accepting connection: %v\n", portNumber, err)
		}
		// serve incoming RPC requests
		go rpc.ServeConn(conn)
	}
}

// starting function for this file --> will be moved to main.go
/*
func main() {
	flag.IntVar(&portNumber, "portNumber", helper.CHUNK_SERVER_START_PORT, "Port number of Chunk Server.")
	flag.Parse()

	// go run chunkserver.go --portNumber 8090-8095
	// Port number arguments

	RunChunkServer(portNumber)
}
*/

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
		log.Printf("[ChunkServer %d] Dialing error: %v\n", cs.portNum, err)
	}

	err = client.Call("MasterNode.RegisterChunkServers", portNum, &response)
	if err != nil {
		log.Printf("[ChunkServer %d] Error calling RPC method: %v\n", cs.portNum, err)
	}
	client.Close()

	log.Printf("[ChunkServer %d] ChunkServer on port: %d. Registration Response %s\n", cs.portNum, portNum, response)

}
