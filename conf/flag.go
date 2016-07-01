package conf

import (
	"flag"
	"strings"

	"github.com/golang/glog"
)

var (
	flagMap    map[string]*flag.Flag
	privateMap map[string]string
	nodeName   string
)

func initFlag(name string) {
	if flagMap == nil {
		flagMap = make(map[string]*flag.Flag)

		if !flag.Parsed() {
			flag.Parse()
		}

		flag.VisitAll(func(f *flag.Flag) {
			flagMap[f.Name] = f
		})
	}
	if nodeName == "" {
		nodeName = name
	}
	if privateMap == nil {
		privateMap = make(map[string]string)
	}
}

func update(name string, value string) {
	if flagMap == nil {
		return
	}

	var prefix string
	var key string
	if pos := strings.Index(name, "."); pos == -1 {
		key = name
	} else {
		prefix = name[0:pos]
		key = name[pos+1:]
	}

	if len(prefix) == 0 {
		updateGeneral(key, value)
		return
	}

	if prefix == nodeName {
		updatePrivate(key, value)
	}
}

func updateGeneral(key string, value string) {
	if privateValue, ok := privateMap[key]; ok {
		glog.Infof("key %s has private value %s, general %s", key, privateValue, value)
		return
	}
	UpdateFlagValue(key, value)
}

func updatePrivate(key string, value string) {
	privateMap[strings.TrimPrefix(key, nodeName)] = value
	UpdateFlagValue(key, value)
}

func UpdateFlagValue(key string, value string) {
	if f, ok := flagMap[key]; ok {
		f.Value.Set(value)
		glog.Infof("update flag %s to %s", key, value)
		return
	}

	glog.Infof("no parameter %s need to be update, value %s", key, value)
}
