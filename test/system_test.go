package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/sutd_gfs_project/client"
	"github.com/sutd_gfs_project/models"
	"github.com/theritikchoure/logx"
)

// Must run master.go first for these tests to work!
// Test if operations still work after killing chunk server

type Task struct {
	Operation int
	Filename  string
	DataSize  int
}

type Client struct {
	ID int
}

const (
	READ   = iota
	APPEND = iota
	WRITE  = iota

	FILE1 = "file1.txt"
	FILE2 = "file2.txt"
	FILE3 = "file3.txt"

	CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n"
)

// Creates a byte array with random characters
// Appends a sentence "Operation executed by Client X" at the end of the data
func generateData(size int, clientId int, t Task) []byte {
	lineDivider := "\n======================================\n"
	// write out start of line to show where file append/writing starts for client
	startOfData := ""
	if t.Operation == APPEND {
		startOfData += lineDivider + "Start of line appended by by Client " + strconv.Itoa(clientId) + lineDivider
	} else if t.Operation == WRITE {
		startOfData += lineDivider + "Start of line written by by Client " + strconv.Itoa(clientId) + lineDivider
	}
	// write out end of line to show where file append/writing ends for client
	endOfData := ""
	if t.Operation == APPEND {
		endOfData += lineDivider + "End of line appended by by Client " + strconv.Itoa(clientId) + lineDivider
	} else if t.Operation == WRITE {
		endOfData += lineDivider + "End of line written by by Client " + strconv.Itoa(clientId) + lineDivider
	}

	startOfDataSize := len(startOfData)
	endOfDataSize := len(endOfData)
	dataSize := size - startOfDataSize - endOfDataSize
	data := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = CHARACTERS[rand.Intn(len(CHARACTERS))]
	}
	data = append([]byte(startOfData), data...)
	data = append(data, []byte(endOfData)...)
	return data
}

func runClient(c *client.Client, t Task) {
	if t.Operation == READ {
		c.ReadFile(t.Filename, 0, 0)
		return
	}

	if t.Operation == APPEND {
		data := generateData(t.DataSize, c.ID, t)
		c.AppendToFile(t.Filename, data)
		return
	}

	if t.Operation == WRITE {
		data := generateData(t.DataSize, c.ID, t)
		c.CreateFile(t.Filename, data)
	}
}

func run(c *client.Client) {
	logx.Logf("[Client %d] Running...", logx.FGBLACK, logx.BGCYAN, c.ID)

	// comment out operations that are not expected to be executed
	if c.ID == 0 {
		runClient(c, Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
		// runClient(c, Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
		runClient(c, Task{Operation: READ, Filename: FILE2})
		// runClient(c, Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
		return
	}
	if c.ID >= 1 {
		time.Sleep(2 * time.Second)
		// runClient(c, Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
		// runClient(c, Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
		runClient(c, Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
		runClient(c, Task{Operation: READ, Filename: FILE2})
		runClient(c, Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
		return
	}
	logx.Logf("[Client %d] Finished running...", logx.FGBLACK, logx.BGGREEN, c.ID)
}
func TestOperationsAfterKillingChunkServer(t *testing.T) {
	fmt.Println("We will be killing one chunkserver for this test")
	c, err := rpc.Dial("tcp", "localhost:8090") // Replace with your master node's address
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer c.Close()

	args := 1
	var reply models.AckSigKill

	err = c.Call("8090.Kill", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}

	fmt.Printf("Killing for chunkserver at 8090 Ack: %v\n", reply.Ack)

	fmt.Printf("Running Write Test after Killing Chunk Server..")
	newClient1 := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data1 := []byte("This the write test.")
	newClient1.CreateFile("testfile1.txt", data1)

	fmt.Printf("Running Read Test after Killing Chunk Server....")
	newClient2 := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	newClient2.ReadFile("testfile1.txt", 0, 0)

	fmt.Printf("Running Append Test after Killing Chunk Server....")
	newClient3 := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data2 := []byte("This is the append test.")
	newClient3.AppendToFile("testfile1.txt", data2)

}

// Test append with 10 clients
func TestMultipleAppend(t *testing.T) {
	var numOfClients int
	flag.IntVar(&numOfClients, "numOfClients", 10, "Number of clients running.")
	flag.Parse()

	for i := 0; i < numOfClients; i++ {
		logfile, err := os.OpenFile("logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("[Client %d] Error opening log file: %v\n", i, err)
		}
		defer logfile.Close()
		log.SetOutput(logfile)

		newClient := client.Client{ID: i, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
		if i == 0 {
			// force Client0 to run first
			run(&newClient)
			continue
		}
		// All other client to run and try to append concurrently
		go run(&newClient)
	}

}
