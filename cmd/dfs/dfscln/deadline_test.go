package main

import (
	"log"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestGetChunkSize(t *testing.T) {
	conn, err := grpc.Dial("192.168.55.193:10000", grpc.WithInsecure())
	if err != nil {
		log.Fatal("dial error")
	}

	size, err := getChunkSize(conn, 4*time.Second)
	if err != nil {
		log.Printf("error %v", err)
		return
	}

	log.Printf("chunk size %d", size)
}
