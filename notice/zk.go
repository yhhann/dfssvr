package notice

import (
	"log"
	"path/filepath"
	"sort"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	// Watchers on ShardServerPath will be noticed when storage server node changed.
	// compatible with old version.
	ShardServerPath = "/shard/server"

	// Watchers on ShardChunkPath will be noticed when segment changed,
	// compatible with old version.
	ShardChunkPath = "/shard/chunk"

	// Watchers on ShardDfsPath will be noticed when other DFSServer changed.
	ShardDfsPath = "/shard/dfs"

	// This two for file migration.
	NodePath   = "/shard/nodes"
	NoticePath = "/shard/notice"
)

// DfsZK implements Notice interface.
type DfsZK struct {
	Addrs []string

	*zk.Conn
}

func (k *DfsZK) connectZk(addrs []string, timeout time.Duration) error {
	k.Addrs = addrs
	thisConn, ch, err := zk.Connect(zk.FormatServers(addrs), timeout)
	if err != nil {
		return err
	}

	connectOk := make(chan struct{})
	go k.checkConnectEvent(ch, connectOk)
	<-connectOk
	k.Conn = thisConn
	return nil
}

// CloseZK closes the zookeeper.
func (k *DfsZK) CloseZk() {
	if k != nil {
		k.Close()
		k = nil
	}
}

func (k *DfsZK) checkConnectEvent(ch <-chan zk.Event, okChan chan<- struct{}) {
	for ev := range ch {
		switch ev.Type {
		case zk.EventSession:
			switch ev.State {
			case zk.StateConnecting:
			case zk.StateConnected:
				log.Printf("Succeeded to connect to zk[%v].", k.Addrs)
			case zk.StateHasSession:
				log.Printf("Succeeded to get session from zk[%v].", k.Addrs)
				okChan <- struct{}{}
			}
		default:
		}
	}
}

// CheckChildren sets a watcher on given path,
// the returned chan will be noticed when children changed.
func (k *DfsZK) CheckChildren(path string) (<-chan []string, <-chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)

	go func() {
		for {
			snapshot, _, events, err := k.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snapshot

			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
			log.Printf("event type: %v", evt.Type)
		}
	}()

	return snapshots, errors
}

// CheckDataChange sets a watcher on given path,
// the returned chan will be noticed when data changed.
func (k *DfsZK) CheckDataChange(path string) (<-chan []byte, <-chan error) {
	datas := make(chan []byte)
	errors := make(chan error)

	go func() {
		for {
			dataBytes, _, events, err := k.GetW(path)
			if err != nil {
				errors <- err
				return
			}
			datas <- dataBytes
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}

		}
	}()

	return datas, errors
}

// GetData returns the data of given path.
func (k *DfsZK) GetData(path string) ([]byte, error) {
	data, _, err := k.Get(path)
	return data, err
}

func (k *DfsZK) createEphemeralSequenceNode(prefix string, data []byte) (string, error) {
	flags := int32(zk.FlagEphemeral | zk.FlagSequence)
	acl := zk.WorldACL(zk.PermAll)
	path, err := k.Create(prefix, data, flags, acl)
	if err != nil {
		return "", err
	} else {
		return path, nil
	}
}

// Register registers a server.
// if check is true, the returned chan will be noticed when sibling changed.
func (k *DfsZK) Register(prefix string, data []byte, startCheckRoutine bool) (string, <-chan []byte, <-chan error, <-chan struct{}, <-chan struct{}) {
	siblings, errs := k.CheckChildren(filepath.Dir(prefix))

	results := make(chan []byte)
	errors := make(chan error)
	clearFlag := make(chan struct{})
	sendFlag := make(chan struct{})

	if startCheckRoutine {
		go func() {
			for {
				select {
				case sn := <-siblings:
					sort.Sort(sort.StringSlice(sn))

					clearFlag <- struct{}{}

					for _, s := range sn {
						path := filepath.Join(filepath.Dir(prefix), s)
						d, err := k.GetData(path)
						if err != nil {
							log.Printf("node lost %v, %v", path, err)
							errors <- err
							continue
						}

						results <- d
					}

					sendFlag <- struct{}{}

				case err := <-errs:
					errors <- err
					return
				}
			}
		}()
	}

	path, err := k.createEphemeralSequenceNode(prefix, data)
	if err != nil {
		errors <- err
	}
	return path, results, errors, clearFlag, sendFlag
}

// NewDfsZK creates a new DfsZk.
func NewDfsZK(addrs []string, timeout time.Duration) *DfsZK {
	zk := new(DfsZK)
	if err := zk.connectZk(addrs, timeout); err != nil {
		return nil
	}
	return zk
}