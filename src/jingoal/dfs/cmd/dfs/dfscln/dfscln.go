package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"jingoal/dfs/client"
	"jingoal/dfs/transfer"
)

var (
	chunkSizeInBytes      = flag.Int("chunk-size", 1024, "chunk size in bytes")
	fileSizeInBytes       = flag.Int("file-size", 1048577, "file size in bytes")
	fileCount             = flag.Int("file-count", 3, "file count")
	domain                = flag.Int64("domain", 2, "domain")
	finalWaitTimeInSecond = flag.Int("final-wait", 10, "the final wait time in seconds")
	version               = flag.Bool("version", false, "print version")

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

	client.Initialize()

	if err := client.AcceptDfsServer(); err != nil {
		log.Printf("accept DfsServer error, %v", err)
	}

	ckSize, err := client.GetChunkSize(int64(*chunkSizeInBytes), 0 /* without timeout */)
	if err != nil {
		log.Printf("negotiate chunk size error, %v", err)
	}
	*chunkSizeInBytes = int(ckSize)

	bizLogicTest(5*time.Second, 100*time.Millisecond, 1000*time.Millisecond)

	if *finalWaitTimeInSecond < 0 {
		select {}
	}

	time.Sleep(time.Duration(*finalWaitTimeInSecond) * time.Second) // For test heartbeat.
}

func bizLogicTest(timeoutLong time.Duration, timeoutShort time.Duration, timeoutWrite time.Duration) {
	fileSize := int64(*fileSizeInBytes)

	domain := int64(5)
	payload := make([]byte, fileSize)

	file, err := client.WriteFile(payload, *chunkSizeInBytes, domain, timeoutWrite)
	if err != nil {
		log.Printf("Failed to write file, %v", err)
	}

	anotherFile, err := client.WriteFile(payload[:], *chunkSizeInBytes, domain, timeoutWrite)
	if err != nil {
		log.Printf("Failed to write file, %v", err)
		return
	}

	aa := anotherFile.Id
	exists(aa, domain, timeoutShort)

	md5 := file.Md5

	r, err := client.ExistByMd5(md5, domain, fileSize, timeoutShort)
	if err != nil {
		log.Printf("Failed to ExistByMd5 %s, error %v", md5, err)
	}
	log.Printf("md5 %s exists %t", md5, r)

	if err := client.ReadFile(file, timeoutShort); err != nil {
		log.Printf("Failed to read file: %v", err)
	}

	a := file.Id
	b, err := client.Duplicate(a, domain, timeoutShort)
	if err != nil {
		log.Printf("Failed to duplicate file %s, error %v", a, err)
	} else {
		for _, f := range []string{a, b} {
			exists(f, domain, timeoutShort)
		}
		log.Printf("Succeeded to duplicate file %s to %s", a, b)
	}

	c, err := client.Copy(b, 10, domain, "1001", "golang-test", timeoutShort)
	if err != nil {
		log.Printf("Failed to copy file %s, error %v", b, err)
	} else {
		exists(c, 10, timeoutShort)
		log.Printf("Succeeded to copy file %s to %s", b, c)
	}

	d, err := client.GetByMd5(md5, domain, fileSize, timeoutShort)
	if err != nil {
		log.Printf("Failed to GetByMd5 md5 %s, error %v", md5, err)
	} else {
		exists(d, domain, timeoutShort)
		log.Printf("Succeeded to GetByMd5 %s, file %s", md5, d)
	}

	for _, f := range []string{a, b, d} {
		err := client.Delete(f, domain, timeoutShort)
		if err != nil {
			log.Printf("Failed to delete file %s, error %v", f, err)
		}
	}

	err = client.Delete(c, 10, timeoutShort)
	if err != nil {
		log.Printf("Failed to delete file %s, error %v", c, err)
	}

	for _, f := range []string{a, b, d} {
		exists(f, domain, timeoutShort)
	}
	exists(c, 10, timeoutShort)

	err = client.Delete(aa, domain, timeoutShort)
	if err != nil {
		log.Printf("Failed to delete file %s, error %v", aa, err)
	}
}

func exists(f string, domain int64, timeout time.Duration) {
	r, err := client.Exists(f, domain, timeout)
	if err != nil {
		log.Printf("Failed to exists file %s, error %v", f, err)
		return
	}
	log.Printf("File %s exists %t", f, r)
}

func performanceTest() {
	timeout := 2 * time.Second

	payload := make([]byte, *fileSizeInBytes)
	files := make(chan *transfer.FileInfo, 10000)
	done := make(chan struct{}, *fileCount)

	go func() {
		for i := 0; i < *fileCount; i++ {
			payload[i] = 0x5A
			file, err := client.WriteFile(payload[:], *chunkSizeInBytes, *domain, timeout)
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
			if err := client.ReadFile(file, timeout); err != nil {
				log.Printf("%v", err)
			}
			done <- struct{}{}
		}
		close(done)
	}()

	for _ = range done {
	}
}
