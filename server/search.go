package server

import (
	"fmt"

	"jingoal.com/dfs/fileop"
)

func (s *DFSServer) openFileForRead(id string, domain int64) (fileop.DFSFileHandler, fileop.DFSFile, error) {
	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		return nil, nil, err
	}

	var m fileop.DFSFileHandler
	if mh != nil {
		m = *mh
	}

	return openFile(id, domain, *nh, m)
}

func openFile(id string, domain int64, nh fileop.DFSFileHandler, mh fileop.DFSFileHandler) (fileop.DFSFileHandler, fileop.DFSFile, error) {
	var h fileop.DFSFileHandler

	if mh != nil && nh != nil {
		h = mh
		file, err := mh.Open(id, domain)
		if err != nil { // Need not to check mgo.ErrNotFound
			h = nh
			file, err = nh.Open(id, domain)
		}
		return h, file, err
	}
	if mh == nil && nh != nil {
		f, err := nh.Open(id, domain)
		return nh, f, err
	}
	return nil, nil, fmt.Errorf("get file error: normal site is nil")
}

func (s *DFSServer) findFileForRead(id string, domain int64) (fileop.DFSFileHandler, string, error) {
	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		return nil, "", err
	}

	var m fileop.DFSFileHandler
	if mh != nil {
		m = *mh
	}

	return findFile(id, *nh, m)
}

func findFile(id string, nh fileop.DFSFileHandler, mh fileop.DFSFileHandler) (fileop.DFSFileHandler, string, error) {
	var h fileop.DFSFileHandler

	if mh != nil && nh != nil {
		h = mh
		fid, err := mh.Find(id)
		if err != nil || fid == "" { // Need not to check mgo.ErrNotFound
			h = nh
			fid, err = nh.Find(id)
		}
		return h, fid, err
	}
	if mh == nil && nh != nil {
		fid, err := nh.Find(id)
		return nh, fid, err
	}
	return nil, "", fmt.Errorf("get file error: normal site is nil")
}
