package metadata

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	DUPL_COL = "fs.dupl"
	RC_COL   = "fs.rc"
)

type Dupl struct {
	Id         bson.ObjectId `bson:"_id"`              // id
	Ref        bson.ObjectId `bson:"reference"`        // reference
	Length     int64         `bson:"length"`           // length
	UploadDate time.Time     `bson:"uploadDate"`       // upload date
	Domain     int64         `bson:"domain,omitempty"` // domain
}

type Ref struct {
	Id         bson.ObjectId `bson:"_id"`        // id
	RefCnt     int64         `bson:"refcnt"`     // referenc count
	Length     int64         `bson:"length"`     // length
	UploadDate time.Time     `bson:"uploadDate"` // upload date
}

type Duplicating struct {
	uri    string
	dbName string

	session *mgo.Session
}

func (d *Duplicating) execute(target func(session *mgo.Session) error) error {
	localSession := d.session.Copy()
	defer localSession.Close()

	return target(localSession)
}

// SaveDupl saves a dupl.
func (d *Duplicating) SaveDupl(dupl *Dupl) error {
	if string(dupl.Id) == "" {
		dupl.Id = bson.NewObjectId()
	}
	if !dupl.Id.Valid() {
		return ObjectIdInvalidError
	}
	if dupl.UploadDate.IsZero() {
		dupl.UploadDate = time.Now()
	}

	return d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(DUPL_COL).Insert(*dupl)
	})
}

// SaveRef saves a reference.
func (d *Duplicating) SaveRef(ref *Ref) error {
	if string(ref.Id) == "" {
		ref.Id = bson.NewObjectId()
	}
	if !ref.Id.Valid() {
		return ObjectIdInvalidError
	}
	if ref.UploadDate.IsZero() {
		ref.UploadDate = time.Now()
	}

	return d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(RC_COL).Insert(*ref)
	})
}

// LookupRefById looks up a ref by its id.
func (d *Duplicating) LookupRefById(id bson.ObjectId) (*Ref, error) {
	ref := new(Ref)
	if err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(RC_COL).FindId(id).One(ref)
	}); err != nil {
		return nil, err
	}

	return ref, nil
}

// LookupDuplById looks up a dupl by its id.
func (d *Duplicating) LookupDuplById(id bson.ObjectId) (*Dupl, error) {
	dupl := new(Dupl)
	if err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(DUPL_COL).FindId(id).One(dupl)
	}); err != nil {
		return nil, err
	}

	return dupl, nil
}

// LookupDuplByRefid looks up a dupl by its ref id.
func (d *Duplicating) LookupDuplByRefid(rid bson.ObjectId) []*Dupl {
	result := make([]*Dupl, 0, 10)

	d.execute(func(session *mgo.Session) error {
		iter := session.DB(d.dbName).C(DUPL_COL).Find(bson.M{"reference": rid}).Iter()
		defer iter.Close()

		for dupl := new(Dupl); iter.Next(dupl); dupl = new(Dupl) {
			result = append(result, dupl)
		}

		return nil
	})

	return result
}

// RemoveDupl removes a dupl by its id.
func (d *Duplicating) RemoveDupl(id bson.ObjectId) error {
	if err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(DUPL_COL).RemoveId(id)
	}); err != nil {
		return err
	}

	return nil
}

// RemoveRef removes a ref by its id.
func (d *Duplicating) RemoveRef(id bson.ObjectId) error {
	if err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(RC_COL).RemoveId(id)
	}); err != nil {
		return err
	}

	return nil
}

// IncRefCnt increases reference count.
func (d *Duplicating) IncRefCnt(id bson.ObjectId) (*Ref, error) {
	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"refcnt": 1,
			},
		},
		ReturnNew: true,
	}

	result := new(Ref)
	if err := d.execute(func(session *mgo.Session) error {
		_, err := session.DB(d.dbName).C(RC_COL).Find(bson.M{"_id": id}).Apply(change, result)
		return err
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// DecRefCnt decreases reference count.
func (d *Duplicating) DecRefCnt(id bson.ObjectId) (*Ref, error) {
	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"refcnt": -1,
			},
		},
		ReturnNew: true,
	}

	result := new(Ref)
	if err := d.execute(func(session *mgo.Session) error {
		_, err := session.DB(d.dbName).C(RC_COL).Find(bson.M{"_id": id}).Apply(change, result)
		return err
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// NewDuplicating creates a Duplicating object with given mongodb uri
// and database name.
func NewDuplicating(dbName string, uri string) (*Duplicating, error) {
	session, err := OpenMongoSession(uri)
	if err != nil {
		return nil, err
	}

	return &Duplicating{
		uri:     uri,
		dbName:  dbName,
		session: session,
	}, nil
}
