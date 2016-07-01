package metadata

import (
	"sync"
	"testing"

	"github.com/golang/glog"
)

func TestSM(t *testing.T) {
	count := 100
	var wg sync.WaitGroup

	sm := NewSessionManager()
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := sm.GetSession("mongodb://192.168.55.193:27017")
			if err != nil {
				glog.Infoln(err)
				return
			}
			glog.Infof("%p", s)
		}()
	}

	wg.Wait()
}
