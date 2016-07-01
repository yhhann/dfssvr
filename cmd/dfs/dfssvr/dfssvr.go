package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"

	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/server"
)

var (
	serverId = flag.String("server-name", "test-dfs-svr", "unique name")

	lsnAddr          = flag.String("listen-addr", ":10000", "listen address")
	zkAddr           = flag.String("zk-addr", "127.0.0.1:2181", "zookeeper address")
	timeout          = flag.Uint("zk-timeout", 15000, "zookeeper timeout")
	shardDbName      = flag.String("shard-name", "shard", "shard database name")
	shardDbUri       = flag.String("shard-dburi", "mongodb://127.0.0.1:27017", "shard database uri")
	eventDbName      = flag.String("event-dbname", "dfsevent", "event database name")
	eventDbUri       = flag.String("event-dburi", "", "event database uri")
	slogDbName       = flag.String("slog-dbname", "dfsslog", "slog database name")
	slogDbUri        = flag.String("slog-dburi", "", "slog database uri")
	logFlushInterval = flag.Uint("log-flush-interval", 10, "interval of glog print in second.")
	compress         = flag.Bool("compress", false, "compressing transfer file")
	concurrency      = flag.Uint("concurrency", 0, "Concurrency")
	version          = flag.Bool("version", false, "print version")

	VERSION   = "2.0"
	buildTime = ""
)

func checkFlags() {
	if *version {
		fmt.Printf("server version: %s-%s\n", VERSION, buildTime)
		os.Exit(0)
	}

	if *serverId == "" {
		glog.Exit("Error: flag --server-name is required.")
	}
	if *lsnAddr == "" {
		glog.Exit("Flag --server-addr is required.")
	}
	if *zkAddr == "" {
		glog.Exit("Flag --zk-addr is required.")
	}
	if *shardDbName == "" {
		glog.Exit("Flag --shard-name is required.")
	}
	if *shardDbUri == "" {
		glog.Exit("Flag --shard-dburi is required.")
	}
	if *slogDbName == "" {
		slogDbName = shardDbName
	}
	if *slogDbUri == "" {
		slogDbUri = shardDbUri
	}
	if *eventDbName == "" {
		eventDbName = shardDbName
	}
	if *eventDbUri == "" {
		eventDbUri = shardDbUri
	}
	if *concurrency < 0 {
		*concurrency = 0
	}
}

// This is a DFSServer instance.
func main() {
	flag.Parse()
	checkFlags()

	go flushLogDaemon()
	defer glog.Flush()

	instrument.StartMetrics()

	lis, err := net.Listen("tcp", *lsnAddr)
	if err != nil {
		glog.Exitf("failed to listen %v", err)
	}
	glog.Infof("DFSServer listened on %s", lis.Addr().String())

	dbAddr := &server.DBAddr{
		ShardDbName: *shardDbName,
		ShardDbUri:  *shardDbUri,
		EventDbName: *eventDbName,
		EventDbUri:  *eventDbUri,
		SlogDbName:  *slogDbName,
		SlogDbUri:   *slogDbUri,
	}

	var dfsServer *server.DFSServer
	for {
		transfer.ServerId = *serverId
		dfsServer, err = server.NewDFSServer(lis.Addr(), *serverId, dbAddr, *zkAddr, *timeout)
		if err != nil {
			glog.Warningf("Failed to create DFS Server: %v, try again.", err)
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

	glog.Flush()
	grpcServer.Serve(lis)
}

func flushLogDaemon() {
	for _ = range time.Tick(time.Duration(*logFlushInterval) * time.Second) {
		glog.Flush()
	}
}
