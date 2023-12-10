package main

import (
	"flag"
	"log"
	"math/rand"
	//"net/rpc"
	"os"
	//"strconv"
	"testing"
	"time"
	"fmt"

	"github.com/sutd_gfs_project/client"
	//"github.com/sutd_gfs_project/models"
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

var DATA = []string{
	"Will you date me?",
	"Breathe if yes.",
	"Recite the Bible in Japanese if no.",
	"はじめに神は天と地とを創造された。",
	"What the...?",
	"地は形なく、むなしく、やみが淵のおもてにあり...",
	"Is that actually the bible?!",
	"...神の霊が水のおもてをおおっていた。",
	"And you stopped breathing, too?",
	"神は「光あれ」と言われた。",
	"I would have preferred you just beat me up and call me gay.",
}

var ok int

func runClient(c *client.Client, t Task, clientID int, loop int) {
	if t.Operation == READ {
		c.ReadFile(t.Filename, 0, 0)
		return
	}

	if t.Operation == APPEND {
		str := fmt.Sprintf("client %d loop %d: %s\n", clientID, loop, DATA[clientID])
		c.AppendToFile(t.Filename, []byte(str))
		return
	}

	if t.Operation == WRITE {
		str := fmt.Sprintf("client %d: %s\n", clientID, DATA[clientID])
		c.CreateFile(t.Filename, []byte(str))
	}
}

func run_append(c *client.Client) {
	logx.Logf("[Client %d] Running...", logx.FGBLACK, logx.BGCYAN, c.ID)

	// comment out operations that are not expected to be executed
	if c.ID == 0 {
		runClient(c, Task{Operation: WRITE, Filename: FILE2}, c.ID, 0)
	}
	if c.ID >= 1 {
		for i := 0; i < 100; i++ {
			amt := time.Duration(rand.Intn(2000))
			time.Sleep(time.Millisecond * amt)
			runClient(c, Task{Operation: APPEND, Filename: FILE2}, c.ID, i)
		}
		ok++
	}
	logx.Logf("[Client %d] Finished running...", logx.FGBLACK, logx.BGGREEN, c.ID)
}

// Test append with 10 clients
func TestMultipleAppend(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	log.Printf("[System: MultipleAppend] Running all clients")

	var numOfClients int
	flag.IntVar(&numOfClients, "numOfClients", 11, "Number of clients running.")
	flag.Parse()

	for i := 0; i < numOfClients; i++ {
		newClient := client.Client{ID: i, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
		if i == 0 {
			// force Client0 to run first
			log.Printf("[System: MultipleAppend] Running Client 0")
			run_append(&newClient)
			continue
		}
		// All other client to run and try to append concurrently
		log.Printf("[System: MultipleAppend] Running Client %d", i)
		go run_append(&newClient)
	}

	for ok < 10 {}
}
