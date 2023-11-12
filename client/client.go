package client // should be client, set temporarily as main so it can be run

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"github.com/sutd_gfs_project/helper"
	"github.com/sutd_gfs_project/models"
)

type Client struct {
	ID        int
	OwnsLease bool
}

/* =============================== Chunk-related functions =============================== */

// Request chunk location from master server
func (c *Client) requestChunkLocation(filename string, chunkIndex int) models.ChunkMetadata {
	client := c.dial(helper.MASTER_SERVER_PORT)
	defer client.Close()

	chunkRequest := models.ChunkLocationArgs{
		Filename:   filename,
		ChunkIndex: chunkIndex,
	}
	var reply models.ChunkMetadata
	err := client.Call("MasterNode.GetChunkLocation", chunkRequest, &reply)
	if err != nil {
		log.Fatalf("[Client %d] Error calling RPC method: %v\n", c.ID, err)
	}

	if chunkIndex != reply.LastIndex {
		log.Printf("[Client %d] Last Index in arguments is incorrect LastIndex of chunkServer", c.ID)
		log.Printf("[Client %d] The last index of chunkServer is: %v\n", c.ID, reply.LastIndex)
	}

	return reply
}

// Read from a chunk in the chunk server
func (c *Client) readChunks(metadata models.ChunkMetadata, index1 int, index2 int) []byte {
	client := c.dial(metadata.Location)
	defer client.Close()

	var reply []byte

	query := models.ReadData{
		ChunkIndex1:   index1,
		ChunkIndex2:   index2,
		ChunkMetadata: metadata,
	}

	err := client.Call("ChunkServer.ReadRange", query, &reply)
	if err != nil {
		log.Fatalf("[Client %d] Error calling RPC method: %v\n", c.ID, err)
	}

	log.Printf("[Client %d] Received chunk: %v\n", c.ID, helper.TruncateOutput(reply))

	return reply
}

/* =============================== File-related functions =============================== */

func (c *Client) ReadFile(filename string, firstIndex int, LastIndex int) {
	// Compute number of chunks
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatalf("[Client %d] Error acquiring file information: %v\n", c.ID, err)
	}
	chunks := computeNumberOfChunks(int(fi.Size()))

	// Read each chunk
	chunkMetadata := c.requestChunkLocation(filename, chunks)
	//chunkMetadata.Handle = helper.StringToUUID("60acd4ca-0ca5-4ba7-b827-dbe81e7529d4")
	c.readChunks(chunkMetadata, firstIndex, LastIndex)
	// TODO: update local copy here
}

// Append to a chunk in the chunk server
// TODO: can append at any point within the existing file
func (c *Client) AppendToFile(filename string, data []byte) {
	leaseArgs := models.LeaseData{FileID: filename, Owner: c.ID, Duration: time.Second * 10}
	var leaseReply models.Lease
	appendArgs := models.Append{Filename: filename, Data: data}
	var appendReply models.AppendData
	// var replicationReply models.ReplicationResponse

	// Sends a request to the master node. This request includes the file name it wants to append data to.
	mnClient := c.dial(helper.MASTER_SERVER_PORT)
	// request for lease first if it does not own lease
	if !c.OwnsLease {
		err := mnClient.Call("MasterNode.CreateLease", leaseArgs, &leaseReply)
		if err != nil {
			// wait for lease to be released --> haven't thought of a waiting loop function or some sort
			log.Printf("[Client %d] Lease for file{%s} request rejected.\n", c.ID, filename)
		}
		c.OwnsLease = true
		// i'm thinking of starting a Go Routine called CheckLeaseExpiration for client, that will check if it's lease has expired or not
		// if it expires, then set c.OwnsLease to false, then attempt to renew by calling API from master
	}
	err := mnClient.Call("MasterNode.Append", appendArgs, &appendReply)
	if err != nil {
		log.Fatalf("[Client %d] Error calling RPC method: %v", c.ID, err)
	}
	mnClient.Close()

	fmt.Println("Masternode Append works")

	// Append data to chunks
	var reply models.Chunk
	csClient := c.dial(helper.CHUNK_SERVER_START_PORT)
	err = csClient.Call("ChunkServer.Append", appendReply, &reply)
	if err != nil {
		log.Fatalf("[Client %d] Error calling RPC method: %v\n", c.ID, err)
	}
	csClient.Close()
	log.Printf("[Client %d] Successfully appended payload %v\n:", c.ID, helper.TruncateOutput(data))

	// Append data to local copy of file
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[Client %d] Error opening file: %v\n", c.ID, err)
	}
	_, err = f.Write(data)
	if err != nil {
		log.Printf("[Client %d] Error writing to file: %v", c.ID, err)
	}
	err = f.Close()
	if err != nil {
		log.Printf("[Client %d] Error closing file: %v\n", c.ID, err)
	}

	// release lease after operation done
	var releaseReply int
	mnClient = c.dial(helper.MASTER_SERVER_PORT)
	err = mnClient.Call("MasterNode.ReleaseLease", leaseArgs, &releaseReply)
	if err != nil {
		log.Fatalf("[Client %d] Error releasing lease for file{%s}: %v\n", c.ID, filename, err)
	}
	c.OwnsLease = false
	mnClient.Close()
}

func (c *Client) CreateFile(filename string, data []byte) error {
	fmt.Println("CREATING FILE")

	// Create the file locally
	err := os.WriteFile(filename, data, 0644)
	if err != nil {
		log.Printf("[Client %d] Error writing to file: %v\n", c.ID, err)
	}

	// Compute number of chunks and prepare args
	var metadata models.ChunkMetadata
	chunks := computeNumberOfChunks(len(data))
	createArgs := models.CreateData{
		Append:         models.Append{Filename: filename, Data: data},
		NumberOfChunks: chunks,
	}

	// Inform master server of new file
	mnClient := c.dial(helper.MASTER_SERVER_PORT)
	err = mnClient.Call("MasterNode.CreateFile", createArgs, &metadata)
	if err != nil {
		log.Fatalf("[Client %d] Error calling RPC method: %v\n", c.ID, err)
	}
	mnClient.Close()

	if metadata.Location == 0 {
		log.Printf("[Client %d] File already exists in chunkserver\n", c.ID)
		return errors.New("file already exists in chunkserver")
	}

	// Split file data into chunks and prepare args
	// Reply has the chunk uuid and location(last index)
	var reply models.SuccessJSON
	var chunkData []byte
	chunkArray := make([]models.Chunk, chunks)
	for i := 0; i < chunks; i++ {
		if i == chunks-1 {
			chunkData = data
		} else {
			chunkData = data[:helper.CHUNK_SIZE]
			data = data[helper.CHUNK_SIZE:]
		}
		chunk := models.Chunk{
			ChunkHandle: metadata.Handle,
			ChunkIndex:  i,
			Data:        chunkData,
		}
		chunkArray[i] = chunk
	}

	// Push chunks to chunk server
	csClient := c.dial(helper.CHUNK_SERVER_START_PORT)
	err = csClient.Call("ChunkServer.CreateFileChunks", chunkArray, &reply)
	if err != nil {
		log.Fatalf("[Client %d] Error calling RPC method: %v\n", c.ID, err)
	}
	csClient.Close()

	log.Printf("[Client %d] Successfully created file in chunkserver: %v\n", c.ID, reply)

	return nil
}

/* =============================== Helper functions =============================== */

func (c *Client) dial(address int) *rpc.Client {
	client, err := rpc.Dial("tcp", "localhost:"+strconv.Itoa(address))
	if err != nil {
		log.Fatalf("[Client %d] Error connecting to RPC server: %v", c.ID, err)
	}
	return client
}

func computeNumberOfChunks(size int) int {
	chunks := size / helper.CHUNK_SIZE
	if size%helper.CHUNK_SIZE > 0 {
		chunks++
	}
	return chunks
}

func main() {
	logfile, err := os.OpenFile("../logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("[Client] Error opening log file:", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)
}
