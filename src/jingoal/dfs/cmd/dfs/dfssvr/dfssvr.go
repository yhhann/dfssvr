package main

import (
	"flag"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

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
)

// This is a DFSServer instance.
func main() {
	flag.Parse()

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

	grpcServer.Serve(lis)
}
