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
	"github.com/theritikchoure/logx"
)

type MasterNode struct {
	ChunkInfo             map[string]models.ChunkMetadata
	Chunks                sync.Map
	LeaseMapping          sync.Map // maps primary replica to lease
	PrimaryReplicaMapping sync.Map // maps fileId to primary replica
	StorageLocation       sync.Map
	Mu                    sync.Mutex
}

/* ============================================ HELPER FUNCTIONS  ===========================================*/
// simple algorithm to find primary replica (find the first alive chunk server)
func (mn *MasterNode) findPrimaryReplica(locations []int, handle string) int {
	var PrimaryReplicaPort int
	for _, value := range locations {
		portAlive, _ := helper.AckMap.Load(value)
		if portAlive == "alive" {
			PrimaryReplicaPort = value
			log.Printf("[Master] Chosen primary replica at chunk server port=%d\n", PrimaryReplicaPort)
			mn.grantLease(PrimaryReplicaPort, models.LeaseDuration)
			mn.PrimaryReplicaMapping.Store(handle, PrimaryReplicaPort)
			return PrimaryReplicaPort
		} else {
			continue
		}
	}
	// no available primary replica (all chunk servers dead)
	return -1
}

// Used for read for chunkserver
func (mn *MasterNode) GetChunkLocation(args models.ChunkLocationArgs, reply *models.MetadataResponse) error {
	var PrimaryReplicaPort int

	// Loads the filename + chunk index to load metadata from key
	chunkMetadata := mn.ChunkInfo[args.Filename]
	if chunkMetadata.Location == nil {
		return helper.ErrChunkNotFound
	}

	value, exists := mn.PrimaryReplicaMapping.Load(args.Filename)
	if exists {
		if primaryReplicaVal, ok := value.(int); ok {
			// primary replica already exists
			PrimaryReplicaPort = primaryReplicaVal
			log.Printf("[Master] Found existing primary replica with port=%d\n", PrimaryReplicaPort)
		}
	} else {
		// primary replica does not exist yet, find one
		PrimaryReplicaPort = mn.findPrimaryReplica(chunkMetadata.Location, args.Filename)
	}

	response := models.MetadataResponse{
		Handle:    chunkMetadata.Handle,
		Location:  PrimaryReplicaPort,
		LastIndex: chunkMetadata.LastIndex,
	}

	*reply = response

	return nil
}

func HeartBeatManager(mn *MasterNode, port int) models.ChunkServerState {
	var reply models.ChunkServerState

	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		log.Println("[Master Heartbeat] Dialing error: ", err)
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
		log.Println("[Master ChunkServer SendHeartBeat] Error calling RPC method: ", err)
	}

	log.Printf("[Master] Heartbeat from ChunkServer %d, Status: %s\n", reply.Port, reply.Status)
	// renew primary replica's lease
	_, exists := mn.LeaseMapping.Load(port)
	if exists {
		mn.RenewLease(port)
	}
	return reply
}

// Will iterate through all chunk servers initialised and ping server with heartbeatManager
func HeartBeatTracker(mn *MasterNode) {
	for {
		for _, port := range helper.ChunkServerPorts {
			output := HeartBeatManager(mn, port)
			if output.Status != "alive" {
				helper.AckMap.Store(port, "dead")

				log.Printf("[Master] Chunk Server at port %d is dead\n", port)
			}
		}

		time.Sleep(2 * time.Second)
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
				log.Println("[Master Replication] Error connecting to RPC server:", err)
				helper.AckMap.Store(port, "dead")
				continue
			} else {
				addChunkArgs := models.Chunk{
					ChunkHandle: args.Chunk.ChunkHandle,
					ChunkIndex:  args.Chunk.ChunkIndex,
					Data:        args.Chunk.Data,
				}

				// Reply with file uuid & last index of chunk
				err = client.Call(fmt.Sprintf("%d.AddChunk", port), addChunkArgs, &output)
				if err != nil {
					log.Println("[Master ChunkServer AddChunk] Error calling RPC method: ", err)
				}
				client.Close()

				response = models.ChunkMetadata{
					Handle:    output.FileID,
					LastIndex: output.LastIndex,
				}
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

	// Verifies that the file exists
	chunkMetadata := mn.ChunkInfo[args.Filename]
	if chunkMetadata.Location == nil {
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

func (mn *MasterNode) CreateFile(args models.CreateData, reply *models.MetadataResponse) error {

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

		*reply = models.MetadataResponse{
			Handle:    uuidNew,
			Location:  metadata.Location[0],
			LastIndex: metadata.LastIndex,
		}
		return nil
	}

	*reply = models.MetadataResponse{}

	return nil
}

/* ============================================ LEASE FUNCTIONS  ===========================================*/
func (mn *MasterNode) grantLease(portNum int, duration time.Duration) {
	// check if lease already exists for the chunk server
	if _, ok := mn.LeaseMapping.Load(portNum); ok {
		// lease already exists, no need to grant new lease
		log.Printf("[Master] Lease already exists for Primary Replica with port=%d\n", portNum)
		return
	}
	newLease := &models.Lease{
		Owner:      portNum,
		Expiration: time.Now().Add(duration), // Expiry Time
		IsExpired:  false,
	}
	mn.LeaseMapping.Store(portNum, newLease)
	log.Printf("[Master] Created lease for Primary Replica with port=%d\n", portNum)
	// grant lease to chunk server serving as new primary replica
	csClient := mn.dial(portNum)
	var reply int
	err := csClient.Call(fmt.Sprintf("%d.GetLease", portNum), newLease, &reply)
	if err != nil {
		log.Printf("[Master] Error granting lease to Primary Replica with port=%d: %v\n", portNum, err)
	}
	csClient.Close()
}

func (mn *MasterNode) RenewLease(primaryReplicaPort int) error {
	mn.Mu.Lock()
	defer mn.Mu.Unlock()
	// check first if lease exist or not
	leaseValue, ok := mn.LeaseMapping.Load(primaryReplicaPort)
	if !ok {
		return fmt.Errorf("[Master] Lease for Primary Replica {port=%d} does not exist\n", primaryReplicaPort)
	}
	// check if lease type is correct
	existingLease, ok := leaseValue.(*models.Lease)
	if !ok {
		return fmt.Errorf("[Master] Unexpected type found in lease for Primary Replica {port=%d}\n", primaryReplicaPort)
	}
	// check if the existing lease has expired or not
	if existingLease.IsExpired || time.Now().After(existingLease.Expiration) {
		existingLease.IsExpired = true
		// if lease has expired, there might be the case that another primary replica has been chosen
		return fmt.Errorf("[Master] Lease for Primary Replica {port=%d} has already expired!\n", primaryReplicaPort)
	}
	// renew the lease if pass all check cases
	existingLease.Expiration = time.Now().Add(10 * time.Second)
	existingLease.IsExpired = false
	// send RenewLease ACK to primary replica
	var reply int
	updatedLease := models.Lease{
		Owner:      existingLease.Owner,
		Expiration: time.Now().Add(models.LeaseDuration),
		IsExpired:  existingLease.IsExpired,
	}
	csClient := mn.dial(primaryReplicaPort)
	err := csClient.Call(fmt.Sprintf("%d.RefreshLease", primaryReplicaPort), updatedLease, &reply)
	if err != nil {
		log.Printf("[Master] Error refreshing lease for Primary Replica with port=%d: %v\n", primaryReplicaPort, err)
	}
	csClient.Close()
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

func (mn *MasterNode) dial(port int) *rpc.Client {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		log.Printf("[Master] Error connecting to RPC server: %v", err)
		return nil
	}
	return client
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

	go HeartBeatTracker(gfsMasterNode)

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
	logx.Logf("Registering port: %d", logx.FGBLUE, logx.BGWHITE, args)

	helper.AckMap.Store(args, "alive")

	helper.ChunkServerPorts = append(helper.ChunkServerPorts, args)

	*reply = "Success"

	return nil
}
