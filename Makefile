export GOPATH := ${GOPATH}:$(shell pwd)

all:
	@echo "make dfs          : build dfs"
	@echo "make tools        : build tools"
	@echo "make fmt          : run go fmt tool"
	@echo "make clean        : clean dfs binary"
	
dfs: fmt
	go install jingoal/dfs/cmd/dfs/dfscln
	go install jingoal/dfs/cmd/dfs/dfssvr
	go install jingoal/dfs/cmd/utils/parsechunks

tools: fmt
	go install jingoal/dfs/cmd/gfapitest

fmt:
#	go fmt jingoal/dfs/cmd/dfs
pb:
	protoc --go_out=plugins=grpc:../transfer src/jingoal/dfs/proto/transfer.proto
	
clean:
	rm -fr bin
	rm -fr pkg
