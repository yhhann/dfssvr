package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"jingoal.com/dfs/client"
	"jingoal.com/dfs/proto/transfer"
)

var (
	chunkSizeInBytes      = flag.Int("chunk-size", 1024, "chunk size in bytes")
	fileSizeInBytes       = flag.Int("file-size", 1048577, "file size in bytes")
	fileCount             = flag.Int("file-count", 3, "file count")
	routineCount          = flag.Int("routine-count", 30, "routine count")
	domain                = flag.Int64("domain", 2, "domain")
	logDir                = flag.String("log-dir", "/var/log/dfs", "The log directory.")
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

func setupLog() {
	if *logDir == "" {
		return
	}

	if _, err := os.Stat(*logDir); os.IsNotExist(err) {
		if err = os.MkdirAll(*logDir, 0700); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}
	}

	f, err := os.OpenFile(filepath.Join(*logDir, "dfs-client.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}

	log.SetOutput(f)
}

// This is a test client for DFSServer, full function client built in Java.
func main() {
	flag.Parse()
	checkFlags()

	setupLog()

	client.Initialize()

	if err := client.AcceptDfsServer(); err != nil {
		log.Printf("accept DfsServer error, %v", err)
	}

	ckSize, err := client.GetChunkSize(int64(*chunkSizeInBytes), 0 /* without timeout */)
	if err != nil {
		log.Printf("negotiate chunk size error, %v", err)
	}
	*chunkSizeInBytes = int(ckSize)

	perfTest()

	if *finalWaitTimeInSecond < 0 {
		select {}
	}

	// For test heartbeat.
	time.Sleep(time.Duration(*finalWaitTimeInSecond) * time.Second)
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
		log.Printf("Failed to exist by md5 %s, %v", md5, err)
	}
	log.Printf("md5 %s exists %t", md5, r)

	if err := client.ReadFile(file, timeoutShort); err != nil {
		log.Printf("Failed to read file: %v", err)
	}

	a := file.Id
	b, err := client.Duplicate(a, domain, timeoutShort)
	if err != nil {
		log.Printf("Failed to duplicate file %s, %v", a, err)
	} else {
		for _, f := range []string{a, b} {
			exists(f, domain, timeoutShort)
		}
		log.Printf("Succeeded to duplicate file %s to %s", a, b)
	}

	c, err := client.Copy(b, 10, domain, "1001", "golang-test", timeoutShort)
	if err != nil {
		log.Printf("Failed to copy file %s, %v", b, err)
	} else {
		exists(c, 10, timeoutShort)
		log.Printf("Succeeded to copy file %s to %s", b, c)
	}

	d, err := client.GetByMd5(md5, domain, fileSize, timeoutShort)
	if err != nil {
		log.Printf("Failed to get by md5 md5 %s, %v", md5, err)
	} else {
		exists(d, domain, timeoutShort)
		log.Printf("Succeeded to get by md5 %s, file %s", md5, d)
	}

	for _, f := range []string{a, b, d} {
		err := client.Delete(f, domain, timeoutShort)
		if err != nil {
			log.Printf("Failed to delete file %s, %v", f, err)
		}
	}

	err = client.Delete(c, 10, timeoutShort)
	if err != nil {
		log.Printf("Failed to delete file %s, %v", c, err)
	}

	for _, f := range []string{a, b, d} {
		exists(f, domain, timeoutShort)
	}
	exists(c, 10, timeoutShort)

	err = client.Delete(aa, domain, timeoutShort)
	if err != nil {
		log.Printf("Failed to delete file %s, %v", aa, err)
	}
}

func exists(f string, domain int64, timeout time.Duration) {
	r, err := client.Exists(f, domain, timeout)
	if err != nil {
		log.Printf("Failed to exists file %s, %v", f, err)
		return
	}
	log.Printf("File %s exists %t", f, r)
}

type Job struct {
	payload []byte
	ckSize  int
}

type Result struct {
	inf *transfer.FileInfo
	err error
}

func perfTest() {
	timeout := 50000 * time.Millisecond
	payload := make([]byte, *fileSizeInBytes)

	jobs := make(chan *Job, *fileCount)
	results := make(chan *Result, *fileCount)
	done := make(chan struct{}, *fileCount)

	for w := 0; w < *routineCount; w++ {
		go func(id int, jobs <-chan *Job, results chan<- *Result) {
			for {
				job, ok := <-jobs
				if !ok {
					break
				}

				info, err := client.WriteFile(job.payload, job.ckSize, *domain, timeout)

				results <- &Result{
					inf: info,
					err: err,
				}
			}
		}(w, jobs, results)
	}

	for j := 0; j < *fileCount; j++ {
		size := rand.Intn(*fileSizeInBytes)
		job := &Job{
			payload: payload[0:size],
			ckSize:  *chunkSizeInBytes,
		}

		jobs <- job
	}

	close(jobs)

	for w := 0; w < *routineCount; w++ {
		go func() {
			for result := range results {
				if result.err != nil {
					continue
				}

				if err := client.ReadFile(result.inf, timeout); err != nil {
					continue
				}
				done <- struct{}{}
			}
		}()
	}

	for m := 0; m < *fileCount; m++ {
		<-done
	}
}
