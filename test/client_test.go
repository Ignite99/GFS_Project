package main

import (
	"fmt"
	"testing"

	"github.com/sutd_gfs_project/client"
)

// Test Writing File
func TestWrite(t *testing.T) {
	fmt.Printf("Running Write Test..")
	newClient := client.Client{ID: 0, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("testing")
	newClient.CreateFile("testfile.txt", data)
}

// Test Reading File from 1 chunk server
func TestRead(t *testing.T) {
	fmt.Printf("Running Read Test..")
	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	newClient.ReadFile("testfile.txt", 0, 0)

}

func TestAppend(t *testing.T) {
	fmt.Printf("Running Append Test..")
	newClient := client.Client{ID: 99, OwnsLease: false, LeaseExpiryChan: make(chan bool, 1), RequestDone: make(chan bool, 1)}
	data := []byte("testing")
	newClient.AppendToFile("testfile.txt", data)
}
