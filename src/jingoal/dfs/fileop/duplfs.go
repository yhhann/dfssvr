package fileop

import (
	"errors"
	"fmt"
	"log"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/metadata"
	"jingoal/dfs/util"
)

var (
	FileNotFound    = errors.New("file not found")
	InvalidObjectId = errors.New("invalid ObjectId")
)

type DuplFs struct {
	gridfs *mgo.GridFS
	op     *metadata.DuplicateOp
}

// Find finds a file with given id.
func (duplfs *DuplFs) Find(givenId string) (*mgo.GridFile, error) {
	realId, err := hexString2ObjectId(util.GetRealId(givenId))
	if err != nil {
		return nil, err
	}

	dupl, err := duplfs.op.LookupDuplById(*realId)
	if err != nil {
		log.Printf("dupl not found: %s", givenId)
		return nil, err
	}
	if dupl != nil {
		return duplfs.gridfs.OpenId(dupl.Ref)
	}

	if !util.IsDuplId(givenId) {
		ref, err := duplfs.op.LookupRefById(*realId)
		if err != nil {
			return nil, err
		}
		if ref == nil {
			return duplfs.gridfs.OpenId(*realId)
		}
	}

	return nil, FileNotFound
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

	dupHex, err := hexString2ObjectId(dupId)
	if err != nil {
		return "", err
	}
	dupl.Id = *dupHex
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

func hexString2ObjectId(hex string) (*bson.ObjectId, error) {
	if bson.IsObjectIdHex(hex) {
		oid := bson.ObjectIdHex(hex)
		return &oid, nil
	}

	return nil, InvalidObjectId
}
