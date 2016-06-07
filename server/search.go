package server

import (
	"fmt"

	"jingoal.com/dfs/fileop"
)

func (s *DFSServer) searchFileForRead(id string, domain int64) (fileop.DFSFileHandler, fileop.DFSFile, error) {
	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		return nil, nil, err
	}

	var m fileop.DFSFileHandler
	if mh != nil {
		m = *mh
	}

	return searchFile(id, domain, *nh, m)
}

func searchFile(id string, domain int64, nh fileop.DFSFileHandler, mh fileop.DFSFileHandler) (fileop.DFSFileHandler, fileop.DFSFile, error) {
	var h fileop.DFSFileHandler

	if mh != nil && nh != nil {
		h = mh
		file, err := mh.Open(id, domain)
		if err != nil { // Need not to check mgo.ErrNotFound
			h = nh
			file, err = nh.Open(id, domain)
		}
		return h, file, err
	} else if mh == nil && nh != nil {
		f, err := nh.Open(id, domain)
		return nh, f, err
	} else {
		return nil, nil, fmt.Errorf("get file error: normal site is nil")
	}
}
