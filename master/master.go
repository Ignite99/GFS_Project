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

var (
	LastPort         int
	firstTimeStepBro int
)

type MasterNode struct {
	ChunkInfo             map[string]models.ChunkMetadata
	Chunks                sync.Map
	PrimaryReplicaMapping sync.Map // maps a ChunkHandle to a primary replica
	LeaseMapping          sync.Map // maps a primary replica to its lease
	StorageLocation       sync.Map
	Mu                    sync.Mutex
}

/* ============================================ HELPER FUNCTIONS  ===========================================*/
// simple algorithm to find primary replica (find the first alive chunk server)
func (mn *MasterNode) findPrimaryReplica(locations []int, chunkHandle uuid.UUID, filename string) int {
	var PrimaryReplicaPort int
	for _, value := range locations {
		portAlive, _ := helper.AckMap.Load(value)
		if portAlive == "alive" {
			PrimaryReplicaPort = value
			log.Printf("[Master] Chosen primary replica at chunk server port=%d for chunkHandle {%v}\n", PrimaryReplicaPort, chunkHandle)
			res := mn.GrantLease(PrimaryReplicaPort, models.LeaseDuration, filename)
			if res == -1 {
				log.Printf("[Master] Chosen primary replica {%d} is dead. Revoke lease and find another one...\n", PrimaryReplicaPort)
				helper.AckMap.Store(value, "dead")
				continue // find another primary replica
			}
			mn.PrimaryReplicaMapping.Store(chunkHandle, PrimaryReplicaPort)
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

	chunkHandle := chunkMetadata.Handle[args.ChunkIndex-1]

	value, exists := mn.PrimaryReplicaMapping.Load(chunkHandle)
	if exists {
		if primaryReplicaVal, ok := value.(int); ok {
			// primary replica already exists
			PrimaryReplicaPort = primaryReplicaVal
			log.Printf("[Master] Found existing primary replica with port=%d\n", PrimaryReplicaPort)
		}
	} else {
		// primary replica does not exist yet, find one
		PrimaryReplicaPort = mn.findPrimaryReplica(chunkMetadata.Location, chunkHandle, args.Filename)
	}

	response := models.MetadataResponse{
		Handle:    chunkMetadata.Handle[args.ChunkIndex-1],
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
	} else {
		log.Printf("[Master] Heartbeat from ChunkServer %d, Status: %s\n", reply.Port, reply.Status)
		if reply.IsPrimaryReplica {
			// renew primary replica's lease
			_, exists := mn.LeaseMapping.Load(port)
			if exists {
				mn.RenewLease(port)
			}
		}
	}
	return reply
}

// Will iterate through all chunk servers initialised and ping server with heartbeatManager
func HeartBeatTracker(mn *MasterNode) {
	for {
		if firstTimeStepBro == 0 {
			time.Sleep(3 * time.Second)
		}

		allDead := true

		for _, port := range helper.ChunkServerPorts {
			output := HeartBeatManager(mn, port)
			if output.Status != "alive" {
				helper.AckMap.Store(port, "dead")

				log.Printf("[Master] Chunk Server at port %d is dead\n", port)
			} else {
				allDead = false
			}
		}

		if allDead {
			log.Println("[Master] All Chunk Servers are dead. Generating new chunk server...")
			go chunkserver.RunChunkServer(LastPort + 1)
			LastPort++
		}

		firstTimeStepBro = 1

		time.Sleep(5 * time.Second)
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
					Handle:    []uuid.UUID{output.FileID},
					LastIndex: output.LastIndex,
				}
			}
		}
	}

	log.Println("[Master] Replication complete to all alive nodes: ", aliveNodes)

	mn.Chunks.Range(func(key, value interface{}) bool {
		if metadata, ok := value.(models.ChunkMetadata); ok {
			if metadata.Handle[len(metadata.Handle)-1] == args.Chunk.ChunkHandle {
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
		Handle:    chunkMetadata.Handle[len(chunkMetadata.Handle)-1],
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
			Handle:    []uuid.UUID{uuidNew},
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
func (mn *MasterNode) GrantLease(portNum int, duration time.Duration, filename string) int {
	// check if lease already exists for the chunk server
	if _, ok := mn.LeaseMapping.Load(portNum); ok {
		// lease already exists, no need to grant new lease
		log.Printf("[Master] Lease already exists for Primary Replica {%d}\n", portNum)
		return 0 // no error found
	}
	newLease := &models.Lease{
		Owner:      portNum,
		Expiration: time.Now().Add(duration), // Expiry Time
		IsExpired:  false,
	}
	mn.LeaseMapping.Store(portNum, newLease)
	log.Printf("[Master] Created lease for Primary Replica {%d}\n", portNum)
	// grant lease to chunk server serving as new primary replica
	csClient, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(portNum))
	if err != nil {
		log.Printf("[Master] Chunk Server {%d} contacted is already dead: %v", portNum, err)
		mn.LeaseMapping.Delete(portNum)
		return -1
	}
	// csClient := mn.dial(portNum)
	var reply int
	err = csClient.Call(fmt.Sprintf("%d.GetLease", portNum), newLease, &reply)
	if err != nil {
		log.Printf("[Master] Error granting lease to Primary Replica {%d}: %v\n", portNum, err)
		return -1
	}
	csClient.Close()
	// start lease timer
	go mn.CheckLeaseExpiry(portNum, newLease, filename)
	return 0 // no error found
}

func (mn *MasterNode) RenewLease(primaryReplicaPort int) {
	// check first if lease exist or not
	leaseValue, ok := mn.LeaseMapping.Load(primaryReplicaPort)
	if !ok {
		log.Printf("[Master] Lease for Primary Replica {%d} does not exist\n", primaryReplicaPort)
		return
	}
	// check if lease type is correct
	existingLease, ok := leaseValue.(*models.Lease)
	if !ok {
		log.Printf("[Master] Unexpected type found in lease for Primary Replica {%d}\n", primaryReplicaPort)
		return
	}
	// check if the existing lease has expired or not
	if existingLease.IsExpired || time.Now().After(existingLease.Expiration) {
		existingLease.IsExpired = true
		// if lease has expired, there might be the case that another primary replica has been chosen
		// will this case even be reached if we have a goroutine timer?
		log.Printf("[Master] Lease for Primary Replica {%d} has already expired!\n", primaryReplicaPort)
		return
	}
	// renew the lease if pass all check cases
	log.Printf("[Master] Renewed lease for Primary Replica {%d}\n", primaryReplicaPort)
	existingLease.Expiration = time.Now().Add(models.LeaseDuration)
	existingLease.IsExpired = false
	// send RenewLease ACK to primary replica
	var reply int
	updatedLease := models.Lease{
		Owner:      existingLease.Owner,
		Expiration: time.Now().Add(models.LeaseDuration),
		IsExpired:  existingLease.IsExpired,
	}
	csClient, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(primaryReplicaPort))
	if err != nil {
		// primary replica is dead
		log.Printf("[Master] Unable to send RefreshLease ACK to dead Primary Replica {%d}\n", primaryReplicaPort)
	} else {
		err = csClient.Call(fmt.Sprintf("%d.RefreshLease", primaryReplicaPort), updatedLease, &reply)
		if err != nil {
			log.Printf("[Master] Error refreshing lease for Primary Replica with port=%d: %v\n", primaryReplicaPort, err)
		}
		csClient.Close()
	}
}

func (mn *MasterNode) CheckLeaseExpiry(primaryReplicaPort int, lease *models.Lease, filename string) {
	// foundExpiredLease := false
	log.Printf("[Master LeaseTimer] Started timer for Primary Replica {%d}\n", primaryReplicaPort)
	for {
		elapsedTime := time.Now().Sub(lease.Expiration)
		if elapsedTime > models.LeaseDuration {
			log.Printf("[Master LeaseTimer] Lease for Primary Replica {%d} has expired!\n", primaryReplicaPort)
			// lease has expired, remove it
			mn.RemoveLease(primaryReplicaPort, lease, filename)
			// exit from goroutine
			return
		}
	}
}

func (mn *MasterNode) RemoveLease(primaryReplicaPort int, lease *models.Lease, filename string) {
	mn.LeaseMapping.Delete(primaryReplicaPort)
	chunkHandles := make([]uuid.UUID, 0)
	// find all chunk handles related to this primary replica
	mn.PrimaryReplicaMapping.Range(func(key, value interface{}) bool {
		chunkHandleKey, ok1 := key.(uuid.UUID)
		primaryReplicaVal, ok2 := value.(int)
		if ok1 && ok2 {
			if primaryReplicaVal == primaryReplicaPort {
				chunkHandles = append(chunkHandles, chunkHandleKey)
			}
		} else {
			log.Printf("[Master LeaseTimer] Key, value type error\n")
		}
		log.Printf("[Master LeaseTimer] Removed lease for Primary Replica {%d}, stop timer\n", primaryReplicaPort)
		return true
	})
	for _, chunkHandle := range chunkHandles {
		mn.PrimaryReplicaMapping.Delete(chunkHandle)
		// find another chunk server to replace as primary replica
		newPrimaryReplica := mn.findPrimaryReplica(mn.ChunkInfo[filename].Location, chunkHandle, filename)
		log.Printf("[Master] New Primary Replica set at Chunk Server {%d}\n", newPrimaryReplica)
	}
	// send revoke to primary replica
	var reply int
	csClient, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(primaryReplicaPort))
	if err != nil {
		// primary replica is dead
		log.Printf("[Master LeaseTimer] Unable to send revoke to dead Primary Replica {%d}\n", primaryReplicaPort)
	} else {
		err = csClient.Call(fmt.Sprintf("%d.RevokeLease", primaryReplicaPort), 1, &reply)
		if err != nil {
			log.Printf("[Master LeaseTimer] Error revoking lease for Primary Replica {%d}: %v\n", primaryReplicaPort, err)
		}
		csClient.Close()
	}
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

	firstTimeStepBro = 0

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

	log.Printf("[Master] RPC server is listening on port %d\n", helper.MASTER_SERVER_PORT)

	for i := 0; i < 3; i++ {
		go chunkserver.RunChunkServer(8090 + i)
	}

	LastPort = 8092

	go HeartBeatTracker(gfsMasterNode)

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
	uuid2 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d3")
	uuid3 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d2")
	uuid4 := helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d1")

	// Handle is the uuid of the metadata that has been assigned to the chunk
	// Location is the chunkServer that it is located in
	metadata1 := models.ChunkMetadata{
		Handle:    []uuid.UUID{uuid1, uuid2, uuid3, uuid4},
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
