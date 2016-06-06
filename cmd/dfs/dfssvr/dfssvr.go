package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"

	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/server"
)

var (
	lsnAddr     = flag.String("listen-addr", ":10000", "listen address")
	name        = flag.String("server-name", "test-dfs-svr", "unique name")
	zkAddr      = flag.String("zk-addr", "127.0.0.1:2181", "zookeeper address")
	timeout     = flag.Int("zk-timeout", 15000, "zookeeper timeout")
	shardDbName = flag.String("shard-name", "shard", "shard database name")
	shardDbUri  = flag.String("shard-dburi", "mongodb://127.0.0.1:27017", "shard database uri")
	logDir      = flag.String("log-dir", "/var/log/dfs", "The log directory.")
	compress    = flag.Bool("compress", false, "compressing transfer file")
	concurrency = flag.Uint("concurrency", 0, "Concurrency")
	version     = flag.Bool("version", false, "print version")

	VERSION   = "2.0"
	buildTime = ""
)

func checkFlags() {
	if *version {
		fmt.Printf("server version: %s-%s\n", VERSION, buildTime)
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
	if *concurrency < 0 {
		*concurrency = 0
	}
}

func setupLog() {
	if *logDir == "" {
		return
	}

	logD := *logDir
	ports := strings.Split(*lsnAddr, ":")
	if len(ports) > 1 {
		logD = filepath.Join(*logDir, ports[1])
	}

	if _, err := os.Stat(logD); os.IsNotExist(err) {
		if err = os.MkdirAll(logD, 0700); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}
	}

	f, err := os.OpenFile(filepath.Join(logD, "dfs-server.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

	instrument.StartMetrics()

	lis, err := net.Listen("tcp", *lsnAddr)
	if err != nil {
		log.Fatalf("failed to listen %v", err)
	}
	log.Printf("DFSServer listened on %s", lis.Addr().String())

	var dfsServer *server.DFSServer
	for {
		dfsServer, err = server.NewDFSServer(lis.Addr(), *name, *shardDbName, *shardDbUri, *zkAddr, *timeout)
		if err != nil {
			log.Printf("Failed to create DFS Server: %v, try again.", err)
			time.Sleep(time.Duration(*server.HealthCheckInterval) * time.Second)
			continue
		}

		break
	}

	sopts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(uint32(*concurrency)),
	}

	if *compress {
		sopts = append(sopts, grpc.RPCCompressor(grpc.NewGZIPCompressor()))
		sopts = append(sopts, grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	}

	grpcServer := grpc.NewServer(sopts...)
	defer grpcServer.Stop()

	transfer.RegisterFileTransferServer(grpcServer, dfsServer)
	discovery.RegisterDiscoveryServiceServer(grpcServer, dfsServer)

	grpcServer.Serve(lis)
}
