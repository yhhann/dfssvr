package discovery

import (
	"fmt"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {
	r := NewZKDfsServerRegister("127.0.0.1:2181", 1*time.Second)

	s := DfsServer{Id: "myServer"}

	if err := r.Register(&s); err != nil {
		t.Errorf("register error %v", err)
	}

	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		for _, v := range r.GetDfsServerMap() {
			fmt.Printf("%v\n", v)
		}
	}

}
