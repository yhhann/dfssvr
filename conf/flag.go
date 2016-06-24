package conf

import (
	"flag"
	"log"
	"strings"
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
		log.Printf("key %s, private %s, general %s", key, privateValue, value)
		return
	}
	up(key, value)
}

func updatePrivate(key string, value string) {
	up(key, value)
	privateMap[strings.TrimPrefix(key, nodeName)] = value
}

func up(key string, value string) {
	if f, ok := flagMap[key]; ok {
		f.Value.Set(value)
		log.Printf("update flag %s to %s", key, value)
		return
	}

	log.Printf("no parameter %s, value %s", key, value)
}
