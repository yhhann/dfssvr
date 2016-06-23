package conf

import (
	"log"
	"path/filepath"
	"strings"

	"github.com/samuel/go-zookeeper/zk"

	"jingoal.com/dfs/notice"
)

const (
	confPath    = "/shard/conf"
	confBufSize = 10000
)

type Conf struct {
	notice notice.Notice
}

func (conf *Conf) startConfUpdateRoutine() {
	routineMap := make(map[string]interface{})
	changes, errs := conf.notice.CheckChildren(confPath)

	kvs := make(chan string, confBufSize)
	go func() {
		for {
			select {
			case confs := <-changes:
				for _, confName := range confs {
					path := filepath.Join(confPath, confName)
					if _, ok := routineMap[path]; !ok {
						vChan, eChan := conf.notice.CheckDataChange(path)
						go func(cn string, p string, vc <-chan []byte, ec <-chan error) {
							for {
								select {
								case v := <-vc:
									kvs <- filepath.Join(cn, string(v))
								case e := <-ec:
									if e == zk.ErrNoNode {
										log.Printf("%v, %s, routine broken", e, cn)
										delete(routineMap, p)
										return
									}
									log.Printf("%v", e)
								}
							}
						}(confName, path, vChan, eChan)

						routineMap[path] = struct{}{}
						log.Printf("Start a routine for %s", confName)
					}
				}
			case err := <-errs:
				log.Printf("%v", err)
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

func NewConf(notice notice.Notice) *Conf {
	initFlag()
	conf := &Conf{
		notice: notice,
	}
	conf.startConfUpdateRoutine()
	return conf
}
