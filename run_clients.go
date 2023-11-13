package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/sutd_gfs_project/client"
)

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
	} else if t.Operation == READ {
		startOfData += lineDivider + "Start of line written by by Client " + strconv.Itoa(clientId) + lineDivider
	}
	// write out end of line to show where file append/writing ends for client
	endOfData := ""
	if t.Operation == APPEND {
		endOfData += lineDivider + "End of line appended by by Client " + strconv.Itoa(clientId) + lineDivider
	} else if t.Operation == READ {
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
		c.ReadFile(t.Filename, 1, 1)
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
	fmt.Printf("[Client %d] Running...\n", c.ID)
	// comment out operations that are not expected to be executed
	if c.ID == 0 {
		runClient(c, Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
		// runClient(c, Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
		runClient(c, Task{Operation: READ, Filename: FILE2})
		runClient(c, Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
		return
	}
	if c.ID == 1 {
		// runClient(c, Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
		// runClient(c, Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
		runClient(c, Task{Operation: READ, Filename: FILE2})
		runClient(c, Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
		return
	}
	if c.ID == 2 {
		// runClient(c, Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
		// runClient(c, Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
		runClient(c, Task{Operation: READ, Filename: FILE2})
		runClient(c, Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
		runClient(c, Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
	}
	fmt.Printf("[Client %d] Finished running...\n", c.ID)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// command line arguments
	var numOfClients int
	flag.IntVar(&numOfClients, "numOfClients", 3, "Number of clients running.")
	flag.Parse()

	for i := 0; i < numOfClients; i++ {
		logfile, err := os.OpenFile("logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("[Client %d] Error opening log file: %v\n", i, err)
		}
		defer logfile.Close()
		log.SetOutput(logfile)

		newClient := client.Client{ID: i, OwnsLease: false}
		if i == 0 {
			// force Client0 to run first
			run(&newClient)
			continue
		}
		// two Clients, 1 and 2, to run and try to append concurrently
		go run(&newClient)
	}

	// prevent program from terminating until CTRL+C command signalled
	var input string
	fmt.Scanln(&input)
}
