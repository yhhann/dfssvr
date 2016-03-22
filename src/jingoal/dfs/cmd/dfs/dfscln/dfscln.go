package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"gopkg.in/mgo.v2/bson"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"jingoal/dfs/discovery"
	"jingoal/dfs/transfer"
)

var (
	serverAddr            = flag.String("server-addr", "127.0.0.1:10000", "server address")
	chunkSizeInBytes      = flag.Int("chunk-size", 1024, "chunk size in bytes")
	fileCount             = flag.Int("file-count", 3, "file count")
	domain                = flag.Int64("domain", 2, "domain")
	finalWaitTimeInSecond = flag.Int("final-wait", 10, "the final wait time in seconds")
	version               = flag.Bool("v", false, "print version")

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
}

// This is a test client for DFSServer, full function client built in Java.
func main() {
	flag.Parse()
	checkFlags()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal("dial error")
	}

	if err := acceptDfsServer(conn); err != nil {
		log.Fatal("accept DfsServer error %v", err)
	}

	ckSize, err := getChunkSize(conn)
	if err != nil {
		log.Fatal("negotiate chunk size error")
	}

	payload := make([]byte, ckSize*10+333)
	files := make(chan *transfer.FileInfo, 10000)
	done := make(chan struct{}, *fileCount)

	go func() {
		for i := 0; i < *fileCount; i++ {
			payload[i] = 0x5A
			file, err := writeFile(conn, payload[:])
			if err != nil {
				log.Printf("%v", err)
				continue
			}
			files <- file
		}

		close(files)
	}()

	go func() {
		for file := range files {
			if err := readFile(conn, file); err != nil {
				log.Printf("%v", err)
			}
			done <- struct{}{}
		}
		close(done)
	}()

	for _ = range done {
	}

	if *finalWaitTimeInSecond < 0 {
		select {}
	}

	time.Sleep(time.Duration(*finalWaitTimeInSecond) * time.Second) // For test heartbeat.
}

func acceptDfsServer(conn *grpc.ClientConn) error {
	client := discovery.NewDiscoveryServiceClient(conn)
	stream, err := client.GetDfsServers(context.Background(),
		&discovery.GetDfsServersReq{
			Client: &discovery.DfsClient{
				Id:  "golang-test-client",
				Uri: "127.0.0.1:0000",
			},
		})
	if err != nil {
		return err
	}

	go func() {
		for {
			rep, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("accept DfsServer error: %v", err)
				continue
			}

			switch union := rep.GetDfsServerUnion.(type) {
			default:
				log.Printf("acceptDfsServer error, unexpected type %T", union)
			case *discovery.GetDfsServersRep_Sl:
				sl := union.Sl.GetServer()
				for _, server := range sl {
					log.Printf("server %+v\n", server)
				}

			// Heartbean can be used for server checking.
			case *discovery.GetDfsServersRep_Hb:
				log.Printf("heartbeat, server timestamp: %d\n", union.Hb.Timestamp)
			}
		}
	}()

	return nil
}

func getChunkSize(conn *grpc.ClientConn) (int64, error) {
	client := transfer.NewFileTransferClient(conn)

	rep, err := client.NegotiateChunkSize(context.Background(),
		&transfer.NegotiateChunkSizeReq{Size: int64(*chunkSizeInBytes)})
	if err != nil {
		return 0, err
	}

	return rep.Size, nil
}

func writeFile(conn *grpc.ClientConn, payload []byte) (*transfer.FileInfo, error) {
	client := transfer.NewFileTransferClient(conn)

	cS, err := client.NegotiateChunkSize(context.Background(),
		&transfer.NegotiateChunkSizeReq{Size: int64(*chunkSizeInBytes)})
	if err != nil {
		return nil, err
	}

	ckSize := cS.Size

	startTime := time.Now()
	fileInfo := transfer.FileInfo{
		Name:   fmt.Sprintf("%v", time.Now().Unix), //*bson.NewObjectId().Hex()*/,
		Size:   ckSize*10 + 333,
		Domain: *domain,
	}

	// PutFile
	stream, err := client.PutFile(context.Background())
	if err != nil {
		return nil, err
	}

	var pos int64
	md5 := md5.New()
	for pos < int64(len(payload)) {
		end := pos + ckSize
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}

		p := payload[pos:end]
		md5.Write(p)
		ck := &transfer.Chunk{Pos: pos,
			Length:  end - pos,
			Payload: p,
		}
		req := &transfer.PutFileReq{Info: &fileInfo, Chunk: ck}
		err := stream.Send(req)
		if err != nil {
			return nil, err
		}
		pos = end
	}

	file, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	info := file.GetFile()
	info.Md5 = fmt.Sprintf("%x", md5.Sum(nil))

	if !bson.IsObjectIdHex(info.Id) {
		log.Printf("write file error: %s\n", info.Id)
		return nil, fmt.Errorf(info.Id)
	}

	elapse := int64(time.Now().Sub(startTime).Seconds())
	log.Printf("write file ok, fileid %s, elapse %d\n", info.Id, elapse)

	return info, nil
}

func readFile(conn *grpc.ClientConn, info *transfer.FileInfo) error {
	client := transfer.NewFileTransferClient(conn)

	_, err := client.NegotiateChunkSize(context.Background(),
		&transfer.NegotiateChunkSizeReq{Size: int64(*chunkSizeInBytes)})
	if err != nil {
		return nil
	}

	startTime := time.Now()

	getFileReq := &transfer.GetFileReq{
		Id:     info.Id,
		Domain: info.Domain,
	}

	// GetFile
	getFileStream, err := client.GetFile(context.Background(), getFileReq)
	if err != nil {
		return err
	}

	md5 := md5.New()
	var fileSize int64
	for {
		ck, err := getFileStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		fileSize += ck.GetChunk().Length
		md5.Write(ck.GetChunk().Payload)
	}

	elapse := int64(time.Now().Sub(startTime).Seconds())
	md5Str := fmt.Sprintf("%x", md5.Sum(nil))
	if md5Str == info.Md5 {
		log.Printf("read file ok, fileid %s, elapse %d\n", info.Id, elapse)
	} else {
		log.Printf("read file error, fileid %s, md5 not equals", info.Id)
	}

	return nil
}
