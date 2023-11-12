package main

import (
	"math/rand"
	"sync"
	"time"
	"os"
	"log"

	"github.com/sutd_gfs_project/client"
)

type Task struct {
	Operation int
	Filename  string
	DataSize  int
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
var wg sync.WaitGroup

// Creates a byte array with random characters
func generateData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = CHARACTERS[rand.Intn(len(CHARACTERS))]
	}
	return data
}

func runClient(t Task) {
	if t.Operation == READ {
		client.ReadFile(t.Filename, 1, 1)
		return
	}

	if t.Operation == APPEND {
		data := generateData(t.DataSize)
		client.AppendToFile(t.Filename, data)
		return
	}

	if t.Operation == WRITE {
		data := generateData(t.DataSize)
		client.CreateFile(t.Filename, data)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	logfile, err := os.OpenFile("logs/master_node.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Println("[Client] Error opening log file: ", err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)

	runClient(Task{Operation: WRITE, Filename: FILE2, DataSize: 65536})
	runClient(Task{Operation: WRITE, Filename: FILE3, DataSize: 66560})
	runClient(Task{Operation: READ, Filename: FILE2})
	go runClient(Task{Operation: APPEND, Filename: FILE2, DataSize: 66000})
	runClient(Task{Operation: READ, Filename: FILE2})
	go runClient(Task{Operation: APPEND, Filename: FILE3, DataSize: 66040})
	go runClient(Task{Operation: APPEND, Filename: FILE3, DataSize: 66080})
	runClient(Task{Operation: READ, Filename: FILE3})
	time.Sleep(time.Second*40)
}
