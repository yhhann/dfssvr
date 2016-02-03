export GOPATH := ${GOPATH}:$(shell pwd)

all: dfs
	@echo "make dfs          : build dfs"
	@echo "make tools        : build tools"
	@echo "make fmt          : run go fmt tool"
	@echo "make clean        : clean dfs binary"

dfs: fmt
	go install jingoal/dfs/cmd/dfs/dfscln
	go install jingoal/dfs/cmd/dfs/dfssvr
	go install jingoal/dfs/cmd/utils/parsechunks
	go install jingoal/dfs/cmd/gfapitest
	go install jingoal/dfs/cmd/study

proto:
	protoc -Isrc --go_out=plugins=grpc:src src/jingoal/dfs/proto/discovery/*.proto
	protoc -Isrc --go_out=plugins=grpc:src src/jingoal/dfs/proto/transfer/*.proto
	cp -a src/jingoal/dfs/proto/discovery/discovery.pb.go src/jingoal/dfs/discovery/discovery.pb.go 
	cp -a src/jingoal/dfs/proto/transfer/transfer.pb.go src/jingoal/dfs/transfer/transfer.pb.go

java:
	protoc --plugin=protoc-gen-grpc-java=/usr/local/bin/protoc-gen-grpc-java --grpc-java_out=../dfs-client/src/main/java --java_out=../dfs-client/src/main/java src/jingoal/dfs/proto/discovery/*.proto
	protoc --plugin=protoc-gen-grpc-java=/usr/local/bin/protoc-gen-grpc-java --grpc-java_out=../dfs-client/src/main/java --java_out=../dfs-client/src/main/java src/jingoal/dfs/proto/transfer/*.proto

debug:
	go install -gcflags "-N -l" jingoal/dfs/cmd/dfs/dfscln
	go install -gcflags "-N -l" jingoal/dfs/cmd/dfs/dfssvr
	go install -gcflags "-N -l" jingoal/dfs/cmd/utils/parsechunks
	go install -gcflags "-N -l" jingoal/dfs/cmd/gfapitest
	go install -gcflags "-N -l" jingoal/dfs/cmd/study

tools: fmt
	go install jingoal/dfs/cmd/gfapitest

fmt:
#	go fmt jingoal/dfs/cmd/dfs

clean:
	rm -fr bin
	rm -fr pkg
