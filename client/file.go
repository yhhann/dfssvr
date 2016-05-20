package client

import (
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/proto/transfer"
)

var (
	WriterAlreadyClosed = errors.New("Writer already closed")
	ReaderAlreadyClosed = errors.New("Reader already closed")
)

// DFSReader implements interface io.ReadCloser for DFS.
type DFSReader struct {
	stream transfer.FileTransfer_GetFileClient
	info   *transfer.FileInfo

	buf    []byte
	offset int64

	closed bool
}

func (r *DFSReader) Read(b []byte) (n int, err error) {
	if r.closed {
		return 0, ReaderAlreadyClosed
	}

	if r.offset == r.info.Size {
		return 0, io.EOF
	}

	for err == nil {
		i := copy(b, r.buf)
		n += i
		r.offset += int64(i)

		r.buf = r.buf[i:]

		if i >= len(b) || r.offset >= r.info.Size {
			break
		}

		b = b[i:]

		ck, err := r.stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, err
		}

		r.buf = ck.GetChunk().Payload
	}

	return
}

func (r *DFSReader) Close() error {
	if !r.closed {
		r.closed = true
		r.offset = 0
	}
	return nil
}

// NewDFSReader returns an object of DFSReader.
func NewDFSReader(stream transfer.FileTransfer_GetFileClient, info *transfer.FileInfo) *DFSReader {
	return &DFSReader{
		stream: stream,
		info:   info,
	}
}

// DFSWriter implements interface io.Writecloser for DFS.
type DFSWriter struct {
	info   *transfer.FileInfo
	stream transfer.FileTransfer_PutFileClient
	md5    hash.Hash
	start  time.Time

	buf []byte
	pos int64

	closed bool
}

func (w *DFSWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, WriterAlreadyClosed
	}

	// TODO(hanyh): split large payload into small one according to the chunkSize.
	return w.write(p)
}

func (w *DFSWriter) write(p []byte) (int, error) {
	size := len(p)

	w.md5.Write(p)
	ck := &transfer.Chunk{Pos: w.pos,
		Length:  int64(size),
		Payload: p,
	}
	req := &transfer.PutFileReq{
		Info:  w.info,
		Chunk: ck,
	}

	err := w.stream.Send(req)
	if err != nil {
		return 0, err
	}
	w.pos += int64(size)

	return size, nil
}

func (w *DFSWriter) Close() error {
	if !w.closed {
		w.closed = true

		rep, err := w.stream.CloseAndRecv()
		if err != nil {
			return err
		}

		info := rep.GetFile()

		if !bson.IsObjectIdHex(info.Id) {
			return fmt.Errorf(info.Id)
		}
		w.info.Id = info.Id
		w.info.Md5 = fmt.Sprintf("%x", w.md5.Sum(nil))

		// TODO(hanyh): monitor the elapse of file transfer.
	}

	return nil
}

// GetFileInfoClose returns an object which hold file information.
// This method would call Close() before return if the writer is
// not closed.
func (w *DFSWriter) GetFileInfoAndClose() (*transfer.FileInfo, error) {
	if !w.closed {
		if err := w.Close(); err != nil {
			return nil, err
		}
	}
	return w.info, nil
}

// NewDFSWriter returns an object of DFSWriter.
func NewDFSWriter(info *transfer.FileInfo, stream transfer.FileTransfer_PutFileClient) *DFSWriter {
	return &DFSWriter{
		info:   info,
		stream: stream,
		md5:    md5.New(),
		start:  time.Now(),
	}
}
