package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sutd_gfs_project/chunkserver"
	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

type MasterNode struct {
	ChunkInfo       map[string]models.ChunkMetadata
	Chunks          sync.Map
	LeaseMapping    sync.Map
	StorageLocation sync.Map
	Mu              sync.Mutex
}

/* ============================================ HELPER FUNCTIONS  ===========================================*/
// Used for read for chunkserver
func (mn *MasterNode) GetChunkLocation(args models.ChunkLocationArgs, reply *models.MetadataResponse) error {
	var PrimaryReplica int

	// Loads the filename + chunk index to load metadata from key
	chunkMetadata := mn.ChunkInfo[args.Filename]
	if chunkMetadata.Location == nil {
		return helper.ErrChunkNotFound
	}

	for _, value := range chunkMetadata.Location {
		portAlive, _ := helper.AckMap.Load(value)
		if portAlive == "alive" {
			PrimaryReplica = value
			break
		} else {
			continue
		}
	}

	response := models.MetadataResponse{
		Handle:    chunkMetadata.Handle,
		Location:  PrimaryReplica,
		LastIndex: chunkMetadata.LastIndex,
	}

	*reply = response

	return nil
}

func HeartBeatManager(port int) models.ChunkServerState {
	var reply models.ChunkServerState

	client, err := rpc.Dial("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Print("[Master] Dialing error: ", err)
		return reply
	}
	defer client.Close()

	heartBeatRequest := models.ChunkServerState{
		LastHeartbeat: time.Now(),
		Status:        "alive",
	}

	log.Println("[Master] Send heartbeat request to chunk")
	err = client.Call(fmt.Sprintf("%d.SendHeartBeat", port), heartBeatRequest, &reply)
	if err != nil {
		log.Println("[Master] Error calling RPC method: ", err)
	}

	log.Printf("[Master] Heartbeat from ChunkServer %d, Status: %s\n", reply.Port, reply.Status)

	/*
		if port == 8090 {
			var ack models.AckSigKill
			client.Call("8090.Kill", 0, &ack)
			fmt.Println("Killing 8090")
		}
	*/
	return reply
}

// Will iterate through all chunk servers initialised and ping server with heartbeatManager
func HeartBeatTracker() {
	for {
		for _, port := range helper.ChunkServerPorts {
			output := HeartBeatManager(port)
			if output.Status != "alive" {
				helper.AckMap.Store(port, "dead")

				log.Printf("[Master] Chunk Server at port %d is dead\n", port)
			}
		}

		time.Sleep(10 * time.Second)
	}
}

// Replicates chunk to all existing chunkServers
// Since all chunkServers anytime they add a chunk they should just replicate
func (mn *MasterNode) Replication(args models.Replication, reply *models.SuccessJSON) error {
	var output models.SuccessJSON
	var response models.ChunkMetadata
	var filename string
	var alivePorts []int

	log.Println("[Master] Replication started")

	aliveNodes := make(map[int]string)

	// Find all alive nodes
	helper.AckMap.Range(func(key, value interface{}) bool {
		if val, ok := value.(string); ok && val == "alive" {
			if port, ok := key.(int); ok {
				aliveNodes[port] = val
				alivePorts = append(alivePorts, port)
			}
		}
		return true
	})

	// Copies data to all alive nodes at that point
	for port := range aliveNodes {
		if port != args.Port {
			fmt.Println("Replicating to port: ", port)

			client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
			if err != nil {
				log.Println("[Master] Error connecting to RPC server:", err)
			}

			addChunkArgs := models.Chunk{
				ChunkHandle: args.Chunk.ChunkHandle,
				ChunkIndex:  args.Chunk.ChunkIndex,
				Data:        args.Chunk.Data,
			}

			// Reply with file uuid & last index of chunk
			err = client.Call(fmt.Sprintf("%d.AddChunk", port), addChunkArgs, &output)
			if err != nil {
				log.Println("[Master] Error calling RPC method: ", err)
			}
			client.Close()

			response = models.ChunkMetadata{
				Handle:    output.FileID,
				LastIndex: output.LastIndex,
			}
		}
	}

	log.Println("[Master] Replication complete to all alive nodes: ", aliveNodes)

	mn.Chunks.Range(func(key, value interface{}) bool {
		if metadata, ok := value.(models.ChunkMetadata); ok {
			if metadata.Handle == args.Chunk.ChunkHandle {
				filename = key.(string)
				return false
			}
		}
		return true
	})

	response.Location = alivePorts

	mn.ChunkInfo[filename] = response

	log.Println("[Master] MasterNode updated of new last index: ", response)

	*reply = output
	return nil
}

/* ============================================ FILE OPERATIONS  ===========================================*/

func (mn *MasterNode) Append(args models.Append, reply *models.AppendData) error {
	var appendArgs models.AppendData
	var PrimaryReplica int

	// The master node receives the client's request for appending data to the file and processes it.
	// It verifies that the file exists and handles any naming conflicts or errors.
	chunkMetadata := mn.ChunkInfo[args.Filename]
	if chunkMetadata.Location == nil {
		// If cannot be found generate new file, call create file
		// mn.CreateFile()
		log.Println("[Master] File does not exist")
	}

	for _, value := range chunkMetadata.Location {
		portAlive, _ := helper.AckMap.Load(value)
		if portAlive == "alive" {
			PrimaryReplica = value
			break
		} else {
			continue
		}
	}

	response := models.MetadataResponse{
		Handle:    chunkMetadata.Handle,
		Location:  PrimaryReplica,
		LastIndex: chunkMetadata.LastIndex,
	}

	appendArgs = models.AppendData{MetadataResponse: response, Data: args.Data}

	*reply = appendArgs

	return nil
}

func (mn *MasterNode) CreateFile(args models.CreateData, reply *models.ChunkMetadata) error {

	if _, exists := mn.ChunkInfo[args.Append.Filename]; !exists {
		log.Println("Key does not exist in the map")

		uuidNew := uuid.NewV4()

		var alivePorts []int

		// Get all alive ports
		helper.AckMap.Range(func(key, value interface{}) bool {
			if val, ok := value.(string); ok && val == "alive" {
				if port, ok := key.(int); ok {
					alivePorts = append(alivePorts, port)
				}
			}
			return true
		})

		metadata := models.ChunkMetadata{
			Handle:    uuidNew,
			Location:  alivePorts,
			LastIndex: args.NumberOfChunks - 1,
		}

		mn.ChunkInfo[args.Append.Filename] = metadata

		*reply = models.ChunkMetadata{
			Handle:    uuidNew,
			Location:  metadata.Location,
			LastIndex: metadata.LastIndex,
		}
		return nil
	}

	*reply = models.ChunkMetadata{}

	return nil
}

/* ============================================ LEASE FUNCTIONS  ===========================================*/
// API called by client
func (mn *MasterNode) CreateLease(args models.LeaseData, reply *models.Lease) error {
	mn.Mu.Lock()
	defer mn.Mu.Unlock()
	// check if lease already exists for the fileID
	if _, ok := mn.LeaseMapping.Load(args.FileID); ok {
		// lease already exists, do not grant new lease
		return fmt.Errorf("[Master] Hey Client%d, lease already exists for fileID {%s}\n", args.Owner, args.FileID)
	}
	newLease := &models.Lease{
		FileID:     args.FileID,
		Owner:      args.Owner,
		Expiration: time.Now().Add(args.Duration), //Expiry Time
		IsExpired:  false,
	}
	mn.LeaseMapping.Store(args.FileID, newLease)
	log.Printf("[Master] Created lease for Client%d for fileId{%s}\n", args.Owner, args.FileID)
	// log.Printf("[Master] Current leases: %v\n", mn.LeaseMapping)
	*reply = models.Lease{
		FileID:     newLease.FileID,
		Owner:      newLease.Owner,
		Expiration: newLease.Expiration,
		IsExpired:  newLease.IsExpired,
	}
	// triggers a warning 'cause I'm copying a lock value (then how else to do it sia for API calls...)
	// placed the lock already for this function
	return nil
}

func (mn *MasterNode) RenewLease(args models.LeaseData, reply *models.Lease) error {
	mn.Mu.Lock()
	defer mn.Mu.Unlock()
	// check first if lease exist or not
	leaseValue, ok := mn.LeaseMapping.Load(args.FileID)
	if !ok {
		return fmt.Errorf("[Master] Hey Client%d, lease for fileID {%s} does not exist\n", args.Owner, args.FileID)
	}
	// check if lease type is correct
	existingLease, ok := leaseValue.(*models.Lease)
	if !ok {
		return fmt.Errorf("[Master] Hey Client%d, unexpected type found in lease for fileID {%s}\n", args.Owner, args.FileID)
	}
	// check if the existing lease hs expired or not
	if existingLease.IsExpired || time.Now().After(existingLease.Expiration) {
		existingLease.IsExpired = true
		return fmt.Errorf("[Master] Hey Client%d, lease for fileID {%s} has already expired!\n", args.Owner, args.FileID)
	}
	// renew the lease if pass all check cases
	existingLease.Expiration = time.Now().Add(args.Duration)
	existingLease.IsExpired = false
	*reply = *existingLease
	return nil
}

func (mn *MasterNode) ReleaseLease(args models.LeaseData, reply *int) error {
	mn.Mu.Lock()
	defer mn.Mu.Unlock()
	// check if lease exists or not
	_, ok := mn.LeaseMapping.Load(args.FileID)
	if !ok {
		return fmt.Errorf("[Master] Hey Client%d, lease for fileID {%s} does not exist\n", args.Owner, args.FileID)
	}
	mn.LeaseMapping.Delete(args.FileID)
	log.Printf("[Master] Released lease for Client%d for fileId{%s}\n", args.Owner, args.FileID)
	// log.Printf("[Master] Lease released: %v\n", value)
	*reply = 1
	return nil
}

func (mn *MasterNode) CheckForExpiredLeases() {
	mn.Mu.Lock()
	defer mn.Mu.Unlock()

	// value type is interface as it is generic
	mn.LeaseMapping.Range(func(key, value interface{}) bool {
		fileID := key.(string)
		lease, ok := value.(*models.Lease)
		if !ok {
			// wrong type for lease value obtained, move on to next entry
			return true
		}
		// check if current lease has expired or not
		if lease.IsExpired || time.Now().After(lease.Expiration) {
			// lease has expired, release it
			mn.LeaseMapping.Delete(fileID)
			log.Printf("[Master]: Lease for fileID {%s} has expired and is released\n", fileID)
			notifyClientAboutExpiredLease(lease, fileID)
		}
		return true
	})
}

// edge case for now
// we assume that client is aware of lease expiration, it will try to renew lease
func notifyClientAboutExpiredLease(lease *models.Lease, fileID string) {
	// haven't implement
	log.Printf("[Master] Notified Client%d of expired lease for fileId{%s}\n", lease.Owner, fileID)
	return
}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("[Master] Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	gfsMasterNode := &MasterNode{
		ChunkInfo: make(map[string]models.ChunkMetadata),
	}
	gfsMasterNode.InitializeChunkInfo()
	gfsMasterNode.LeaseMapping = sync.Map{}

	rpc.Register(gfsMasterNode)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(helper.MASTER_SERVER_PORT))
	if err != nil {
		log.Println("[Master] Error starting RPC server:", err)
	}
	defer listener.Close()

	go HeartBeatTracker()

	log.Printf("[Master] RPC server is listening on port %d\n", helper.MASTER_SERVER_PORT)

	for i := 0; i < 3; i++ {
		go chunkserver.RunChunkServer(8090 + i)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[Master] Error accepting connection: %v", err)
			continue
		}

		go rpc.ServeConn(conn)
	}
}

/* ============================================ INITIALISING DATA  ===========================================*/

func (mn *MasterNode) InitializeChunkInfo() {

	uuid1 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")

	// Handle is the uuid of the metadata that has been assigned to the chunk
	// Location is the chunkServer that it is located in
	metadata1 := models.ChunkMetadata{
		Handle:    uuid1,
		Location:  []int{8090, 8091, 8092},
		LastIndex: 3,
	}

	mn.ChunkInfo["file1.txt"] = metadata1
}

func (mn *MasterNode) RegisterChunkServers(args int, reply *string) error {
	fmt.Println("Registering port: ", args)

	helper.AckMap.Store(args, "alive")

	helper.ChunkServerPorts = append(helper.ChunkServerPorts, args)

	*reply = "Success"

	return nil
}
