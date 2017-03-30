package fileop

import (
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2/bson"

	dra "jingoal.com/dfs/cassandra"
	"jingoal.com/dfs/util"
)

// DuplDra processes operations about file duplication.
// The file duplication metadata is stored in cassandra.
type DuplDra struct {
	*dra.MetaOp
}

// FindByMd5 finds a file by its md5.
func (dupldra *DuplDra) FindByMd5(md5 string, domain int64) (*dra.File, error) {
	return dupldra.LookupFileByMd5(md5, domain)
}

// Find finds a file with given id.
func (dupldra *DuplDra) Find(givenId string) (f *dra.File, err error) {
	f, err = dupldra.search(givenId)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, FileNotFound
	}

	return f, nil
}

func (dupldra *DuplDra) search(givenId string) (*dra.File, error) {
	if !util.IsDuplId(givenId) {
		return dupldra.LookupFileById(givenId)
	}

	dupl, err := dupldra.LookupDuplById(util.GetRealId(givenId))
	if err != nil {
		return nil, err
	}

	if dupl == nil {
		return nil, FileNotFound
	}

	return dupldra.LookupFileById(dupl.Ref)
}

// Duplicate duplicates an entry for a file, not the content.
func (dupldra *DuplDra) Duplicate(oid string) (string, error) {
	return dupldra.DuplicateWithId(oid, "", time.Time{})
}

// DuplicateWithId duplicates an entry for a file with given file id, not the content.
func (dupldra *DuplDra) DuplicateWithId(oid string, dupId string, createDate time.Time) (string, error) {
	primary, err := dupldra.search(oid)
	if err != nil {
		return "", err
	}

	ref, err := dupldra.saveRefAndDuplIfAbsent(primary.Id, primary.Size, primary.Domain)
	if err != nil {
		return "", err
	}
	_, err = dupldra.IncRefCnt(ref.Id)
	if err != nil {
		return "", err
	}

	dupl := dra.Dupl{
		Id:         dupId,
		Ref:        ref.Id,
		Length:     primary.Size,
		Domain:     primary.Domain,
		CreateDate: createDate,
	}

	if err := dupldra.SaveDupl(&dupl); err != nil {
		return "", err
	}

	return util.GetDuplId(dupl.Id), nil
}

func (dupldra *DuplDra) saveRefAndDuplIfAbsent(pid string, size int64, domain int64) (*dra.Ref, error) {
	ref, err := dupldra.LookupRefById(pid)
	if err != nil {
		return nil, err
	}
	if ref != nil {
		return ref, nil
	}

	nref := &dra.Ref{
		Id:     pid,
		RefCnt: 0,
	}
	if err := dupldra.SaveRef(nref); err != nil {
		return nil, err
	}

	dupl := &dra.Dupl{
		Id:     pid,
		Ref:    nref.Id,
		Length: size,
		Domain: domain,
	}
	if err := dupldra.SaveDupl(dupl); err != nil {
		return nil, err
	}

	return nref, nil
}

// LazyDelete deletes a duplication or a real file.
// It returns true when deletes a real file successfully.
func (dupldra *DuplDra) LazyDelete(dId string) (bool, string, error) {
	realId := util.GetRealId(dId)

	dupl, err := dupldra.LookupDuplById(realId)
	if err != nil {
		return false, "", err
	}

	if dupl == nil {
		return dupldra.delFile(dId, realId)
	}

	return dupldra.delFileAndDupl(dupl)
}

func (dupldra *DuplDra) delFile(did string, entityId string) (bool, string, error) {
	if util.IsDuplId(did) {
		glog.V(5).Infof("Try to delete a file %s but it's a dupl, ignored.", did)
		return false, "", nil
	}

	ref, err := dupldra.LookupRefById(entityId)
	if err != nil {
		return false, "", err
	}
	if ref == nil {
		return true, entityId, nil
	}
	glog.V(5).Infof("Try to delete a file %s but it's a dupl, ignored.", did)

	return false, "", nil
}

func (dupldra *DuplDra) delFileAndDupl(dupl *dra.Dupl) (bool, string, error) {
	err := dupldra.RemoveDupl(dupl.Id)
	if err != nil {
		return false, "", err
	}

	status, err := dupldra.decAndRemove(dupl.Ref)
	if err != nil {
		return false, "", err
	}
	if status < 0 {
		return true, dupl.Ref, nil
	}

	return false, "", nil
}

func (dupldra *DuplDra) decAndRemove(id string) (int64, error) {
	ref, err := dupldra.DecRefCnt(id)
	if err != nil {
		return 0, err
	}
	if ref == nil {
		dupldra.RemoveRef(id)
		return -1, nil
	}

	if ref.RefCnt < 0 {
		dupldra.RemoveRef(id)
	}

	return ref.RefCnt, nil
}

func (dupldra *DuplDra) LookupFileMeta(id string) (*FileMeta, error) {
	f, err := dupldra.LookupFileById(id)
	if err != nil {
		return nil, err
	}

	return &FileMeta{
		Id:         bson.ObjectIdHex(f.Id),
		ChunkSize:  f.ChunkSize,
		UploadDate: f.UploadDate,
		Length:     f.Size,
		MD5:        f.Md5,
		Filename:   f.Name,
		Domain:     f.Domain,
		UserId:     f.UserId,
		Biz:        f.Biz,
	}, nil
}

func NewDuplDra(metaOp *dra.MetaOp) *DuplDra {
	return &DuplDra{
		MetaOp: metaOp,
	}
}
