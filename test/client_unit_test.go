package main

import (
	"log"
	"math"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sutd_gfs_project/client"
	"github.com/sutd_gfs_project/models"
)

func AtoIArray(_array []string) []int {
	var output []int
	for _, val := range _array {
		val = strings.TrimSpace(val)
		intTime, err := strconv.Atoi(val)
		if err == nil {
			output = append(output, intTime)
		}
	}
	return output
}

func MinMax(array []int) (int, int) {
	var max int = array[0]
	var min int = array[0]
	for _, value := range array {
		if max < value {
			max = value
		}
		if min > value {
			min = value
		}
	}
	return min, max
}


func DetermineOperationTime(startTimesContent string, endTimesContent string) (float64){
	startTimes := AtoIArray(strings.Split(startTimesContent, "\n"))
	endTimes := AtoIArray(strings.Split(endTimesContent, "\n"))
	startTime, _ := MinMax(startTimes)
	_, endTime := MinMax(endTimes)
	duration := float64((endTime - startTime))
	duration = duration / math.Pow(10, 9)
	return duration
}

// Must run master.go first for these tests to work!
// Test Writing File
func TestWrite(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	log.Printf("[Client: Write] Running Write Test..")
	newClient := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("This the write test.")

	startTime:= strconv.Itoa(int(time.Now().UnixNano()))
	newClient.CreateFile("testfile.txt", data)
	endTime:=strconv.Itoa(int(time.Now().UnixNano()))

	dur := DetermineOperationTime(startTime, endTime)
	log.Printf("[Client: Write] Duration of operation: %fs", dur)
}

// Test Reading File from 1 chunk server
func TestRead(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	log.Printf("[Client: Read] Running Read Test..")
	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	
	startTime:= strconv.Itoa(int(time.Now().UnixNano()))
	newClient.ReadFile("testfile.txt", 0, 0)
	endTime:= strconv.Itoa(int(time.Now().UnixNano()))

	dur := DetermineOperationTime(startTime, endTime)
	log.Printf("[Client: Read] Duration of operation: %fs", dur)
}

// Test Reading File from another chunk server
// Need to hardcode a chunk server to read from, but how?
// func TestReadReplica(t *testing.T) {
// 	fmt.Printf("Running Read Test..")
// 	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}

// }

// Test Appending Content
func TestAppend(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	log.Printf("[Client: Append] Running Append Test..")
	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("This is the append test.")

	startTime:= strconv.Itoa(int(time.Now().UnixNano()))
	newClient.AppendToFile("testfile.txt", data)
	endTime:= strconv.Itoa(int(time.Now().UnixNano()))

	dur := DetermineOperationTime(startTime, endTime)
	log.Printf("[Client: Append] Duration of operation: %fs", dur)
}

func TestKillChunkServer(t *testing.T) {
	logfile, _ := os.OpenFile("../logs/testing.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logfile.Close()
	log.SetOutput(logfile)

	log.Printf("[Client: KillChunkServer] We will be killing one chunkserver for this test")
	client, err := rpc.Dial("tcp", "localhost:8090") // Replace with your master node's address
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	defer client.Close()

	args := 1
	var reply models.AckSigKill

	err = client.Call("8090.Kill", args, &reply)
	if err != nil {
		log.Fatal("Error calling RPC method: ", err)
	}

	log.Printf("[Client: KillChunkServer] Killing for chunkserver at 8090 Ack: %v\n", reply.Ack)

}
