package cassandra

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/relops/cqlr"
	"gopkg.in/mgo.v2/bson"
)

const (
	// table name.
	FILES_COL = "files"
	DUPL_COL  = "dupl"
	RC_COL    = "rc"
)

const (
	EntityNone EntityType = iota
	EntityGlusterFS
	EntityGridFS
	EntitySeadraFS
)

const (
	cqlSaveDupl          = `INSERT INTO dupl (id, domain, size, refid, cdate) VALUES (?, ?, ?, ?, ?)`
	cqlLookupDuplById    = `SELECT * FROM dupl WHERE id = ?`
	cqlLookupDuplByRefid = `SELECT * FROM dupl WHERE refid = ?`
	cqlRemoveDupl        = `DELETE FROM dupl WHERE id = ?`
	cqlLookupRefById     = `SELECT * FROM rc WHERE id = ?`
	cqlRemoveRef         = `DELETE FROM rc WHERE id = ?`
	cqlUpdateRefCnt      = `UPDATE rc SET refcnt = refcnt + %d WHERE id = ?`
	cqlLookupFileById    = `SELECT * FROM files where id = ?`
	calLookupFileByMd5   = `SELECT * FROM md5 where md5 = ?`
	cqlSaveFile          = `INSERT INTO files (id, biz, cksize, domain, fn, size, md5, udate, uid, type, attrs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	cqlRemoveFile        = `DELETE FROM files where id = ?`
)

var (
	FileNotFound = errors.New("file not found")
)

// Type of storage which stores file content.
type EntityType uint8

// File represents the metadata of a DFS file.
type File struct {
	Id         string            `cql:"id"`
	Biz        string            `cql:"biz"`
	ChunkSize  int               `cql:"cksize"`
	Domain     int64             `cql:"domain"`
	Name       string            `cql:"fn"`
	Size       int64             `cql:"size"`
	Md5        string            `cql:"md5"`
	UploadDate time.Time         `cql:"udate"`
	UserId     string            `cql:"uid"`
	Metadata   map[string]string `cql:"attrs"`
	Type       EntityType        `cql:"type"`
}

// Dupl represents the metadata of a file duplication.
type Dupl struct {
	Id         string    `cql:"id"`
	Ref        string    `cql:"refid"`
	Length     int64     `cql:"size"`
	CreateDate time.Time `cql:"cdate"`
	Domain     int64     `cql:"domain"`
}

// Ref represents another metadata of a file duplication.
type Ref struct {
	Id     string `cql:"id"`
	RefCnt int64  `cql:"refcnt"`
}

type MetaOp struct {
	*gocql.ClusterConfig

	session *gocql.Session
	lock    sync.Mutex
}

// SaveDupl saves a dupl.
func (op *MetaOp) SaveDupl(dupl *Dupl) error {
	if len(dupl.Id) == 0 {
		dupl.Id = bson.NewObjectId().Hex()
	}
	if dupl.CreateDate.IsZero() {
		dupl.CreateDate = time.Now()
	}

	return op.execute(func(session *gocql.Session) error {
		b := cqlr.Bind(cqlSaveDupl, dupl)
		return b.Exec(session)
	})
}

// LookupDuplById looks up a dupl by its id.
func (op *MetaOp) LookupDuplById(id string) (*Dupl, error) {
	dupl := &Dupl{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupDuplById, id)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(dupl)

		if len(dupl.Id) == 0 {
			dupl = nil
		}

		return nil
	})

	return dupl, err
}

// LookupDuplByRefid looks up a dupl by its ref id. No use right now.
func (op *MetaOp) LookupDuplByRefid(rid string) []*Dupl {
	result := make([]*Dupl, 0, 10)

	op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupDuplByRefid, rid)
		b := cqlr.BindQuery(q)
		defer b.Close()

		dupl := &Dupl{}
		for b.Scan(dupl) {
			if len(dupl.Id) == 0 {
				continue
			}
			result = append(result, dupl)
		}

		return nil
	})

	return result
}

// RemoveDupl removes a dupl by its id.
func (op *MetaOp) RemoveDupl(id string) error {
	return op.execute(func(session *gocql.Session) error {
		return session.Query(cqlRemoveDupl, id).Exec()
	})
}

// SaveRef saves a reference.
func (op *MetaOp) SaveRef(ref *Ref) error {
	if len(ref.Id) == 0 {
		return errors.New("id of ref is nil.")
	}

	return op.addRefCnt(ref.Id, 0)
}

// LookupRefById looks up a ref by its id.
func (op *MetaOp) LookupRefById(id string) (*Ref, error) {
	ref := &Ref{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupRefById, id)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(ref)

		if len(ref.Id) == 0 {
			ref = nil
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ref, nil
}

// RemoveRef removes a ref by its id.
func (op *MetaOp) RemoveRef(id string) error {
	return op.execute(func(session *gocql.Session) error {
		return session.Query(cqlRemoveRef, id).Exec()
	})
}

// IncRefCnt increases reference count.
func (op *MetaOp) IncRefCnt(id string) (*Ref, error) {
	r, err := op.LookupRefById(id)
	if err != nil {
		return nil, err
	}

	err = op.addRefCnt(id, 1)
	if err != nil {
		return nil, err
	}

	if r != nil {
		r.RefCnt++
		return r, nil
	}

	return op.LookupRefById(id)
}

// DecRefCnt decreases reference count.
func (op *MetaOp) DecRefCnt(id string) (*Ref, error) {
	r, err := op.LookupRefById(id)
	if err != nil {
		return nil, err
	}

	err = op.addRefCnt(id, -1)
	if err != nil {
		return nil, err
	}

	if r != nil {
		r.RefCnt--
		return r, nil
	}

	return op.LookupRefById(id)
}

func (op *MetaOp) addRefCnt(id string, delta int) error {
	query := fmt.Sprintf(cqlUpdateRefCnt, delta)
	return op.execute(func(session *gocql.Session) error {
		return session.Query(query, id).Consistency(gocql.All).Exec()
	})
}

// LookupFileById looks up a file by its id.
func (op *MetaOp) LookupFileById(id string) (*File, error) {
	f := &File{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupFileById, id)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(f)

		if len(f.Id) == 0 {
			f = nil
			return FileNotFound
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return f, nil
}

// LookupFileByMd5 looks up a file by its md5.
func (op *MetaOp) LookupFileByMd5(md5 string, domain int64) (*File, error) {
	f := &File{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(calLookupFileByMd5, md5)
		b := cqlr.BindQuery(q)
		defer b.Close()
		for b.Scan(f) {
			if f.Domain == domain {
				break
			}
		}

		if len(f.Id) == 0 {
			f = nil
			return FileNotFound
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return f, nil
}

// SaveFile saves a file.
func (op *MetaOp) SaveFile(f *File) error {
	if f.Type == EntityNone {
		return errors.New("File type unknown.")
	}

	return op.execute(func(session *gocql.Session) error {
		b := cqlr.Bind(cqlSaveFile, f)
		return b.Exec(session)
	})
}

// RemoveFile removes a file by its id.
func (op *MetaOp) RemoveFile(id string) error {
	return op.execute(func(session *gocql.Session) error {
		return session.Query(cqlRemoveFile, id).Exec()
	})
}

// getSession returns a cql session.
// It makes dfs server can start before cassandra.
func (op *MetaOp) getSession() (*gocql.Session, error) {
	if op.session != nil {
		return op.session, nil
	}

	op.lock.Lock()
	defer op.lock.Unlock()

	if op.session != nil {
		return op.session, nil
	}

	var err error
	op.session, err = op.CreateSession()

	return op.session, err
}

func (op *MetaOp) execute(target func(session *gocql.Session) error) error {
	session, err := op.getSession()
	if err != nil {
		return err
	}

	return target(session)
}

// NewMetaOp returns a MetaOp object with given parameters.
func NewMetaOp(seeds []string, sqlOptions ...func(*MetaOp)) *MetaOp {
	// TODO(hanyh): Consider re-use letsgo/cql/conf to create cluster.
	self := &MetaOp{
		ClusterConfig: gocql.NewCluster(seeds...),
	}

	for _, option := range sqlOptions {
		option(self)
	}

	return self
}
