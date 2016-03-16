package metadata

import "testing"

func TestSaveAndRemoveRef(t *testing.T) {
	dup, err := NewDuplicating("dupl-test", dbUri)
	if err != nil {
		t.Errorf("NewDuplicating error %v", err)
	}

	ref := Ref{
		Length: 2099,
	}

	err = dup.SaveRef(&ref)
	if err != nil {
		t.Errorf("SaveRef error %v", err)
	}

	lookedRef, err := dup.LookupRefById(ref.Id)
	if err != nil {
		t.Errorf("LookupRefById error %v", err)
	}

	if lookedRef.RefCnt != ref.RefCnt {
		t.Errorf("LookupRefById error: refCnt not equals")
	}

	err = dup.RemoveRef(ref.Id)
	if err != nil {
		t.Errorf("RemoveRef error %v", err)
	}

	_, err = dup.LookupRefById(ref.Id)
	if err == nil {
		t.Errorf("LookupRefById error %v", err)
	}
}

func TestIncDecRef(t *testing.T) {
	dup, err := NewDuplicating("dupl-test", dbUri)
	if err != nil {
		t.Errorf("NewDuplicating error %v", err)
	}

	ref := Ref{
		Length: 2099,
	}

	err = dup.SaveRef(&ref)
	if err != nil {
		t.Errorf("SaveRef error %v", err)
	}

	lookedRef, err := dup.LookupRefById(ref.Id)
	if err != nil {
		t.Errorf("LookupRefById error %v", err)
	}

	if lookedRef.RefCnt != 0 {
		t.Errorf("LookupRefById error: refCnt not zero.")
	}

	incRef, err := dup.IncRefCnt(ref.Id)
	if err != nil {
		t.Errorf("IncRefCnt error: %v", err)
	}
	if incRef.RefCnt != 1 {
		t.Errorf("IncRefCnt error: refCnt not 1, but %d", incRef.RefCnt)
	}

	decRef, err := dup.DecRefCnt(ref.Id)
	if err != nil {
		t.Errorf("DecRefCnt error: %v", err)
	}
	if decRef.RefCnt != 0 {
		t.Errorf("DecRefCnt error: refCnt not zero, but %d", decRef.RefCnt)
	}

	err = dup.RemoveRef(ref.Id)
	if err != nil {
		t.Errorf("RemoveRef error %v", err)
	}
}

func TestDupl(t *testing.T) {
	op, err := NewDuplicating("dupl-test", dbUri)
	if err != nil {
		t.Errorf("NewDuplicating error %v", err)
	}

	ref := Ref{
		Length: 2099,
	}

	err = op.SaveRef(&ref)
	if err != nil {
		t.Errorf("SaveRef error %v", err)
	}
	dupl := Dupl{
		Ref:    ref.Id,
		Length: ref.Length,
		Domain: 2,
	}

	err = op.SaveDupl(&dupl)
	if err != nil {
		t.Errorf("SaveDupl error %v", err)
	}

	lookedDupl, err := op.LookupDuplById(dupl.Id)
	if err != nil {
		t.Errorf("LookupDuplbyId error %v", err)
	}
	if lookedDupl.UploadDate.UnixNano()/1000000 != dupl.UploadDate.UnixNano()/1000000 {
		t.Errorf("LookupDuplbyId error, not equal: %d--%d",
			lookedDupl.UploadDate.UnixNano()/1000000, dupl.UploadDate.UnixNano()/1000000)
	}

	dupls := op.LookupDuplByRefid(ref.Id)
	if err != nil {
		t.Errorf("LookupDuplbyRefid error %v", err)
	}
	if len(dupls) <= 0 {
		t.Errorf("LookupDuplbyRefid error: nothing.")
	}

	err = op.RemoveDupl(dupl.Id)
	if err != nil {
		t.Errorf("RemoveDupl error %v", err)
	}

	err = op.RemoveRef(ref.Id)
	if err != nil {
		t.Errorf("RemoveRef error %v", err)
	}
}
