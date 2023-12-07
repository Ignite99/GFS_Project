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
	"github.com/theritikchoure/logx"
)

//var portNumber int

type ChunkServer struct {
	Storage         []models.Chunk
	PortNum         int
	Lease           models.Lease
	LeaseExpiryChan chan bool
	Killed          bool
}

/* =============================== Chunk Storage functions =============================== */
// Master will talk with these Chunk functions to create, update, delete, or replicate chunks

// get chunk from storage
func (cs *ChunkServer) GetChunk(chunkHandle uuid.UUID, chunkLocation int) models.Chunk {
	var chunkRetrieved models.Chunk

	// Only iterate through those with relevant uuid. Will only enter upon detecting ChunkLocation is similar to chunkIndex
	for _, val := range cs.Storage {
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
	log.Printf("[ChunkServer %d] Chunk added: {%v %d %s}\n", cs.PortNum, args.ChunkHandle, args.ChunkIndex, helper.TruncateOutput(args.Data))
	cs.Storage = append(cs.Storage, args)
	index := len(cs.Storage)

	*reply = models.SuccessJSON{
		FileID:    args.ChunkHandle,
		LastIndex: index,
	}
	return nil
}

func (cs *ChunkServer) CreateFileChunks(args []models.Chunk, reply *models.SuccessJSON) error {
	var successResponse models.SuccessJSON

	log.Println("============== CREATE CHUNKS IN CHUNK SERVER ==============")
	logMessage := fmt.Sprintf("[ChunkServer %d] Chunks added: ", cs.PortNum)

	for _, c := range args {
		cs.Storage = append(cs.Storage, c)

		newChunk := models.Chunk{
			ChunkHandle: c.ChunkHandle,
			ChunkIndex:  c.ChunkIndex,
			Data:        c.Data,
		}

		replicateChunk := models.Replication{
			Port:  cs.PortNum,
			Chunk: newChunk,
		}

		client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
		if err != nil {
			log.Printf("[ChunkServer %d] Dialing error: %v\n", cs.PortNum, err)
		}

		err = client.Call("MasterNode.Replication", replicateChunk, &successResponse)
		if err != nil {
			log.Printf("[ChunkServer %d Replication] Error calling RPC method: %v\n", cs.PortNum, err)
		}
		client.Close()

		log.Printf("[ChunkServer %d] Successful Replication: %v\n", cs.PortNum, successResponse)

		logMessage += fmt.Sprintf("\n{%v %d %s}", c.ChunkHandle, c.ChunkIndex, helper.TruncateOutput(c.Data))
	}
	log.Println(logMessage)

	index := len(cs.Storage)

	*reply = models.SuccessJSON{
		FileID:    args[0].ChunkHandle,
		LastIndex: index,
	}
	return nil
}

// update value of chunk in storage
func (cs *ChunkServer) UpdateChunk(args models.Chunk, reply *models.Chunk) error {
	var updatedChunk models.Chunk
	for idx, val := range cs.Storage {
		if val.ChunkHandle == args.ChunkHandle {
			cs.Storage[idx] = args
			updatedChunk = cs.Storage[idx]
			break
		}
	}
	*reply = updatedChunk
	return nil
}

// delete chunk from storage
func (cs *ChunkServer) DeleteChunk(args models.Chunk, reply *models.Chunk) error {
	var deletedChunk models.Chunk
	for idx, val := range cs.Storage {
		if val.ChunkHandle == args.ChunkHandle {
			cs.Storage = append(cs.Storage[:idx], cs.Storage[:idx+1]...)
			deletedChunk = args
			break
		}
	}
	*reply = deletedChunk
	return nil
}

/* ============================ Lease functions ============================ */
// if chunk server selected as primary replica, get lease from master
// returns its own port number
func (cs *ChunkServer) GetLease(args models.Lease, reply *int) error {
	cs.Lease = models.Lease{
		Owner:      args.Owner,
		Expiration: args.Expiration,
		IsExpired:  args.IsExpired,
	}
	cs.LeaseExpiryChan = make(chan bool, 1)
	*reply = cs.PortNum
	return nil
}

// master to call this when it master receives heartbeat from chunk server
// something like ACK for refreshLease called within SendHeartBeat
func (cs *ChunkServer) RefreshLease(args models.Lease, reply *int) error {
	// update new expiration time
	cs.Lease.Expiration = args.Expiration
	cs.Lease.IsExpired = false
	*reply = cs.PortNum
	return nil
}

// master calls this to revoke lease
func (cs *ChunkServer) RevokeLease(args int, reply *int) error {
	log.Printf("[ChunkServer %d] Lease as primary replica revoked\n", cs.PortNum)
	cs.Lease.IsExpired = true
	*reply = cs.PortNum
	return nil
}

/* ============================ ChunkServer functions ============================ */

// chunk server to reply heartbeat to master to verify that it is alive
func (cs *ChunkServer) SendHeartBeat(args models.ChunkServerState, reply *models.ChunkServerState) error {
	heartBeat := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        args.Status,
		Node:          helper.CHUNK_SERVER_START_PORT,
		Port:          cs.PortNum,
	}
	if !cs.Lease.IsExpired {
		heartBeat.IsPrimaryReplica = true
	} else {
		heartBeat.IsPrimaryReplica = false
	}
	*reply = heartBeat
	return nil
}

// client to call this API when it wants to read data
func (cs *ChunkServer) ReadRange(args models.ReadData, reply *[]byte) error {
	var dataStream []byte

	chunkUUID := args.ChunkMetadata.Handle

	if args.ChunkIndex1 == args.ChunkIndex2 {
		for _, chunk := range cs.Storage {
			if chunk.ChunkHandle == chunkUUID && chunk.ChunkIndex == args.ChunkIndex1 {
				dataStream = append(dataStream, chunk.Data...)
			}
		}
	} else {
		for _, chunk := range cs.Storage {
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
	chunk := cs.GetChunk(args.MetadataResponse.Handle, args.MetadataResponse.LastIndex)
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
			chunk = models.Chunk{ChunkHandle: args.MetadataResponse.Handle, ChunkIndex: index, Data: []byte{}}
			if len(args.Data) <= chunkSpace {
				chunk.Data = append(chunk.Data, args.Data...)
				args.Data = nil
			} else {
				chunk.Data = append(chunk.Data, args.Data[:chunkSpace]...)
				args.Data = args.Data[chunkSpace:]
			}

			replicateChunk := models.Replication{Port: cs.PortNum, Chunk: chunk}

			// Updates Master for new last index entry
			client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
			if err != nil {
				log.Printf("[ChunkServer %d] Dialing error: %v\n", cs.PortNum, err)
			}

			err = client.Call("MasterNode.Replication", replicateChunk, &successResponse)
			if err != nil {
				log.Printf("[ChunkServer %d] Error calling RPC method: %v\n", cs.PortNum, err)
			}
			client.Close()

			log.Printf("[ChunkServer %d] Successful Replication: %v\n", cs.PortNum, successResponse)
		}
	}

	*reply = chunk
	return nil
}

func (cs *ChunkServer) Kill(args int, reply *models.AckSigKill) error {
	cs.Killed = true
	*reply = models.AckSigKill{Ack: true}
	log.Printf("[ChunkServer %d] Received kill signal\n", cs.PortNum)
	return nil
}

// command or API call for MAIN function to run chunk server
func RunChunkServer(portNumber int) {
	// initialize chunk server instance
	chunkServerInstance := &ChunkServer{
		Storage: make([]models.Chunk, 0),
		PortNum: portNumber,
	}
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
		if chunkServerInstance.Killed {
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

func (cs *ChunkServer) dial(port int) *rpc.Client {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		log.Printf("[Client %d] Error connecting to RPC server: %v", cs.PortNum, err)
		return nil
	}
	return client
}

func (cs *ChunkServer) InitialiseChunks() {
	logx.Logf("Initialised chunkServer Data", logx.FGBLUE, logx.BGWHITE)
	uuid1 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")
	uuid2 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d3")
	uuid3 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d2")
	uuid4 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d1")

	chunk1 := models.Chunk{
		ChunkHandle: uuid1,
		ChunkIndex:  0,
		Data:        []byte("Hello"),
	}
	chunk2 := models.Chunk{
		ChunkHandle: uuid2,
		ChunkIndex:  1,
		Data:        []byte("World"),
	}
	chunk3 := models.Chunk{
		ChunkHandle: uuid3,
		ChunkIndex:  2,
		Data:        []byte("Foo"),
	}
	chunk4 := models.Chunk{
		ChunkHandle: uuid4,
		ChunkIndex:  3,
		Data:        []byte("Bar"),
	}

	cs.Storage = append(cs.Storage, chunk1, chunk2, chunk3, chunk4)
}

func (cs *ChunkServer) Registration(portNum int) {
	var response string

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Printf("[ChunkServer %d] Dialing error: %v\n", cs.PortNum, err)
	}

	err = client.Call("MasterNode.RegisterChunkServers", portNum, &response)
	if err != nil {
		log.Printf("[ChunkServer %d Registration] Error calling RPC method: %v\n", cs.PortNum, err)
	}
	client.Close()

	log.Printf("[ChunkServer %d] ChunkServer on port: %d. Registration Response %s\n", cs.PortNum, portNum, response)

}
