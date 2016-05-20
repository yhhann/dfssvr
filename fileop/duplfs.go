package fileop

import (
	"errors"
	"fmt"
	"log"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/util"
)

var (
	FileNotFound = errors.New("file not found")
)

type DuplFs struct {
	gridfs *mgo.GridFS
	op     *metadata.DuplicateOp
}

// FindByMd5 finds a gridfs file by its md5.
func (duplfs *DuplFs) FindByMd5(md5 string, domain int64, size int64) (*mgo.GridFile, error) {
	file := new(mgo.GridFile)

	query := bson.D{
		{"domain", domain},
		{"md5", md5},
		{"length", size},
	}

	iter := duplfs.gridfs.Find(query).Sort("-uploadDate").Iter()
	defer iter.Close()

	if ok := duplfs.gridfs.OpenNext(iter, &file); ok {
		return file, nil
	}

	return nil, FileNotFound
}

// Find finds a file with given id.
func (duplfs *DuplFs) Find(givenId string) (f *mgo.GridFile, err error) {
	defer func() {
		if err == mgo.ErrNotFound {
			err = FileNotFound
		}
	}()

	realId, err := hexString2ObjectId(util.GetRealId(givenId))
	if err != nil {
		return
	}

	dupl, err := duplfs.op.LookupDuplById(*realId)
	if err != nil {
		log.Printf("dupl not found: %s, error: %v", givenId, err)
		return
	}
	if dupl != nil {
		f, err = duplfs.gridfs.OpenId(dupl.Ref)
		return
	}

	if !util.IsDuplId(givenId) {
		var ref *metadata.Ref
		ref, err = duplfs.op.LookupRefById(*realId)
		if err != nil {
			log.Printf("ref not found: %s, error: %v", givenId, err)
			return
		}
		if ref == nil {
			f, err = duplfs.gridfs.OpenId(*realId)
			return
		}
	}

	err = FileNotFound
	return
}

func (duplfs *DuplFs) search(fid string) (*mgo.GridFile, error) {
	if !util.IsDuplId(fid) {
		givenId, err := hexString2ObjectId(fid)
		if err != nil {
			return nil, err
		}

		return duplfs.gridfs.OpenId(givenId)
	}

	rId := util.GetRealId(fid)
	realId, err := hexString2ObjectId(rId)
	if err != nil {
		return nil, err
	}

	dupl, err := duplfs.op.LookupDuplById(*realId)
	if err != nil {
		return nil, err
	}
	if dupl == nil || !dupl.Ref.Valid() {
		return nil, FileNotFound
	}

	return duplfs.gridfs.OpenId(dupl.Ref)
}

// Duplicate duplicates an entry for a file, not the content.
func (duplfs *DuplFs) Duplicate(oid string) (string, error) {
	return duplfs.DuplicateWithId(oid, "", time.Now())
}

// DuplicateWithId duplicates an entry for a file with given file id, not the content.
func (duplfs *DuplFs) DuplicateWithId(oid string, dupId string, uploadDate time.Time) (string, error) {
	primary, err := duplfs.search(oid)
	if err != nil {
		return "", err
	}

	pid, ok := primary.Id().(bson.ObjectId)
	if !ok {
		return "", fmt.Errorf("primary id invalided: %v", primary.Id())
	}

	ref, err := duplfs.op.LookupRefById(pid)
	if err != nil {
		return "", err
	}
	if ref == nil {
		ref = &metadata.Ref{
			Id:     pid,
			Length: primary.Size(),
			RefCnt: 1,
		}
		if err := duplfs.op.SaveRef(ref); err != nil {
			return "", err
		}

		nDupl := metadata.Dupl{
			Id:     pid,
			Ref:    ref.Id,
			Length: primary.Size(),
		}
		if err := duplfs.op.SaveDupl(&nDupl); err != nil {
			return "", err
		}
	} else {
		_, err := duplfs.op.IncRefCnt(ref.Id)
		if err != nil {
			return "", err
		}
	}

	dupl := metadata.Dupl{
		Ref:    ref.Id,
		Length: ref.Length,
	}

	if dupId != "" {
		dupHex, err := hexString2ObjectId(dupId)
		if err != nil {
			return "", err
		}
		dupl.Id = *dupHex
	}

	dupl.UploadDate = uploadDate

	if err := duplfs.op.SaveDupl(&dupl); err != nil {
		return "", err
	}

	return util.GetDuplId(dupl.Id.Hex()), nil
}

// Delete deletes a duplication or a real file.
// It returns true when deletes a real file successfully.
func (duplfs *DuplFs) Delete(dId string) (bool, error) {
	var status int64
	var result bool

	realId, err := hexString2ObjectId(util.GetRealId(dId))
	if err != nil {
		return false, err
	}

	dupl, err := duplfs.op.LookupDuplById(*realId)
	if err != nil {
		return false, err
	}

	if dupl == nil {
		if util.IsDuplId(dId) {
			status = -10000
		} else {
			ref, err := duplfs.op.LookupRefById(*realId)
			if err != nil {
				return false, err
			}
			if ref == nil {
				duplfs.gridfs.RemoveId(realId)
				result = true
			} else {
				status = -20000
			}
		}
	} else {
		err := duplfs.op.RemoveDupl(dupl.Id)
		if err != nil {
			return false, err
		}

		status, err = duplfs.decAndRemove(dupl.Ref)
		if err != nil {
			return false, err
		}
		if status < 0 {
			result = true
		}
	}

	// TODO(hanyh): Log this delete event for audit.
	return result, nil
}

func (duplfs *DuplFs) decAndRemove(id bson.ObjectId) (int64, error) {
	ref, err := duplfs.op.DecRefCnt(id)
	if err == mgo.ErrNotFound {
		duplfs.op.RemoveRef(id)
		duplfs.gridfs.RemoveId(id)
		return -1, nil
	}
	if err != nil {
		return 0, err
	}

	if ref.RefCnt < 0 {
		duplfs.op.RemoveRef(id)
		duplfs.gridfs.RemoveId(id)
	}

	return ref.RefCnt, nil
}

func NewDuplFs(gridfs *mgo.GridFS, dOp *metadata.DuplicateOp) *DuplFs {
	duplfs := &DuplFs{
		gridfs: gridfs,
		op:     dOp,
	}

	return duplfs
}

type FileMeta struct {
	Id          interface{} "_id"
	ChunkSize   int         "chunkSize"
	UploadDate  time.Time   "uploadDate"
	Length      int64       "length,minsize"
	MD5         string      "md5"
	Filename    string      "filename,omitempty"
	ContentType string      "contentType,omitempty"

	Domain int64  "domain"
	UserId string "userid"
	Biz    string "bizname"
}

func LookupFileMeta(gridfs *mgo.GridFS, query bson.D) (*FileMeta, error) {
	iter := gridfs.Find(query).Sort("-uploadDate").Iter()
	defer iter.Close()

	fm := new(FileMeta)
	if iter.Next(fm) {
		return fm, nil
	}

	return nil, FileNotFound
}

func hexString2ObjectId(hex string) (*bson.ObjectId, error) {
	if bson.IsObjectIdHex(hex) {
		oid := bson.ObjectIdHex(hex)
		return &oid, nil
	}

	return nil, fmt.Errorf("Invalid ObjectId: %s", hex)
}
