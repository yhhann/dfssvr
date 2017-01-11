package conf

import (
	"path/filepath"
	"strings"

	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"

	"jingoal.com/dfs/notice"
)

const (
	confBufSize = 10000
)

// Conf holds the config for flag.
type Conf struct {
	notice   notice.Notice
	prefix   string
	confPath string
}

func (conf *Conf) startConfUpdateRoutine() {
	routineMap := make(map[string]interface{})
	changes, errs := conf.notice.CheckChildren(conf.confPath)

	kvs := make(chan string, confBufSize)
	go func() {
		for {
			select {
			case confs := <-changes:
				for _, confName := range confs {
					// filetered by prefix.
					if !strings.HasPrefix(confName, conf.prefix) {
						continue
					}

					path := filepath.Join(conf.confPath, confName)
					if _, ok := routineMap[path]; !ok {
						vChan, eChan := conf.notice.CheckDataChange(path)
						go func(cn string, p string, vc <-chan []byte, ec <-chan error) {
							for {
								select {
								case v := <-vc:
									kvs <- filepath.Join(strings.TrimPrefix(cn, conf.prefix), string(v))
								case e := <-ec:
									if e == zk.ErrNoNode {
										glog.V(3).Infof("%v, %s, watcher routine stopped.", e, cn)
										delete(routineMap, p)
										return
									}
									glog.Warningf("%v", e)
								}
							}
						}(confName, path, vChan, eChan)

						routineMap[path] = struct{}{}
						glog.V(3).Infof("Start a routine for %s.", confName)
					}
				}
			case err := <-errs:
				glog.Warningf("%v", err)
			}
		}
	}()

	go func() {
		for {
			pair := <-kvs
			kv := strings.Split(pair, "/")
			if len(kv) >= 2 {
				update(kv[0], kv[1])
			}
		}
	}()
}

func NewConf(confPath string, prefix string, nodeName string, notice notice.Notice) *Conf {
	initFlag(nodeName)
	conf := &Conf{
		confPath: confPath,
		prefix:   prefix,
		notice:   notice,
	}
	conf.startConfUpdateRoutine()
	return conf
}
