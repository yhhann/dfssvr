package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
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
	bufSize               = flag.Int("buf-size", 8192, "size of buffer to read and write")
	fileCount             = flag.Int("file-count", 3, "file count")
	routineCount          = flag.Int("routine-count", 30, "routine count")
	domain                = flag.Int64("domain", 2, "domain")
	logDir                = flag.String("log-dir", "/var/log/dfs", "The log directory.")
	finalWaitTimeInSecond = flag.Int("final-wait", 10, "the final wait time in seconds")
	version               = flag.Bool("version", false, "print version")

	VERSION = "2.0"
)

func checkFlags() {
	if *version {
		fmt.Printf("client version: %s\n", VERSION)
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

	bizLogicTest(1*time.Second, 5*time.Second, 10*time.Second)

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

	file, err := WriteFile(payload, *chunkSizeInBytes, domain, timeoutWrite)
	if err != nil {
		log.Printf("Failed to write file, %v", err)
	}

	anotherFile, err := WriteFile(payload[:], *chunkSizeInBytes, domain, timeoutWrite)
	if err != nil {
		log.Printf("Failed to write file, %v", err)
		return
	}

	aa := anotherFile.Id
	exists(aa, domain, timeoutShort)

	info, err := client.Stat(anotherFile.Id, anotherFile.Domain, timeoutShort)
	if err != nil {
		log.Printf("File not found %s, %d, %v", anotherFile.Id, anotherFile.Domain, err)
	}

	log.Printf("Found file %v", info)

	md5 := file.Md5

	r, err := client.ExistByMd5(md5, domain, fileSize, timeoutShort)
	if err != nil {
		log.Printf("Failed to exist by md5 %s, %v", md5, err)
	}
	log.Printf("md5 %s exists %t", md5, r)

	if err := ReadFile(file, timeoutShort); err != nil {
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
	timeout := 1500 * time.Millisecond
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

				info, err := WriteFile(job.payload, job.ckSize, *domain, timeout)

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
				if result.err == nil { // ignore error.
					ReadFile(result.inf, timeout)
				}
				done <- struct{}{}
			}
		}()
	}

	for m := 0; m < *fileCount; m++ {
		<-done
	}
}

// WriteFile writes a file for given content. Only for test use.
func WriteFile(payload []byte, chunkSize int, domain int64, timeout time.Duration) (info *transfer.FileInfo, err error) {
	size := int64(len(payload))
	fn := fmt.Sprintf("%d", time.Now().UnixNano())
	start := time.Now()
	var writer *client.DFSWriter

	writer, err = client.GetWriter(domain, size, fn, "test-biz", "1001", timeout)
	if err != nil {
		return
	}
	defer writer.Close()

	var pos int64
	md5 := md5.New()
	for pos < size {
		end := pos + int64(chunkSize)
		if end > size {
			end = size
		}

		p := payload[pos:end]
		md5.Write(p)

		var n int
		n, err = writer.Write(p)
		if err != nil {
			if err == io.EOF {
				return writer.GetFileInfoAndClose()
			}
			log.Printf("write file error %v", err)
			return
		}
		pos += int64(n)
	}

	info, err = writer.GetFileInfoAndClose()

	if err != nil {
		log.Printf("write file error %v", err)
		return
	}

	// TODO(hanyh): Save the success event in client metrics.
	log.Printf("write file ok, file %v, elapse %f second\n", info, time.Since(start).Seconds())
	return info, nil
}

// ReadFile reads a file content. Only for test use.
func ReadFile(info *transfer.FileInfo, timeout time.Duration) (err error) {
	startTime := time.Now()

	defer func() {
		if err != nil {
			log.Printf("read file error, file %s, %v", info.Id, err)
		} else {
			log.Printf("read file ok, file %s, elapse %f seconds.", info.Id, time.Since(startTime).Seconds())
		}
	}()

	var reader *client.DFSReader
	reader, err = client.GetReader(info.Id, info.Domain, timeout)
	if err != nil {
		return
	}

	buf := make([]byte, *bufSize)
	md5 := md5.New()
	for {
		var n int
		n, err = reader.Read(buf)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}

		md5.Write(buf[:n])
	}

	md5Str := fmt.Sprintf("%x", md5.Sum(nil))
	if md5Str != info.Md5 {
		err = fmt.Errorf("md5 not equals")
	}

	return
}
