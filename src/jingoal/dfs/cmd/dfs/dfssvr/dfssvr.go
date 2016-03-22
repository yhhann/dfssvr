package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"jingoal/dfs/discovery"
	"jingoal/dfs/server"
	"jingoal/dfs/transfer"
)

var (
	lsnAddr     = flag.String("server-addr", ":10000", "listen address")
	name        = flag.String("server-name", "test-dfs-svr", "unique name")
	zkAddr      = flag.String("zk-addr", "127.0.0.1:2181", "zookeeper address")
	timeout     = flag.Int("zk-timeout", 15000, "zookeeper timeout")
	shardDbName = flag.String("shard-name", "shard", "shard database name")
	shardDbUri  = flag.String("shard-dburi", "mongodb://127.0.0.1:27017", "shard database uri")
	logDir      = flag.String("log-dir", "/var/log/dfs", "The log directory.")
	version     = flag.Bool("v", false, "print version")

	buildTime = ""
)

func checkFlags() {
	if buildTime == "" {
		log.Println("Error: Build time not set!")
		os.Exit(0)
	}

	if *version {
		fmt.Printf("Build time: %s\n", buildTime)
		os.Exit(0)
	}

	if *name == "" {
		log.Println("Error: flag --server-name is required.")
		os.Exit(1)
	}
	if *lsnAddr == "" {
		log.Println("Flag --server-addr is required.")
		os.Exit(2)
	}
	if *zkAddr == "" {
		log.Println("Flag --zk-addr is required.")
		os.Exit(3)
	}
	if *shardDbName == "" {
		log.Println("Flag --shard-name is required.")
		os.Exit(4)
	}
	if *shardDbUri == "" {
		log.Println("Flag --shard-dburi is required.")
		os.Exit(5)
	}
}

func setupLog() {
	if *logDir == "" {
		return
	}

	if _, err := os.Stat(*logDir); os.IsNotExist(err) {
		if err = os.MkdirAll(*logDir, 0700); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}
	}
	f, err := os.OpenFile(filepath.Join(*logDir, "dfs-server.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}

	log.SetOutput(f)
}

// This is a DFSServer instance.
func main() {
	flag.Parse()
	checkFlags()

	setupLog()

	lis, err := net.Listen("tcp", *lsnAddr)
	if err != nil {
		grpclog.Fatalf("failed to listen %v", err)
	}

	cs, err := server.NewDFSServer(lis.Addr(), *name, *shardDbName, *shardDbUri, *zkAddr, *timeout)
	if err != nil {
		grpclog.Fatalf("create NewDFSServer failed%v", err)
	}

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	transfer.RegisterFileTransferServer(grpcServer, cs)
	discovery.RegisterDiscoveryServiceServer(grpcServer, cs)

	grpcServer.Serve(lis)
}
