package conf

import (
	"flag"
	"log"
)

var (
	flagMap map[string]*flag.Flag
)

func initFlag() {
	if flagMap == nil {
		flagMap = make(map[string]*flag.Flag)

		if !flag.Parsed() {
			flag.Parse()
		}

		flag.VisitAll(func(f *flag.Flag) {
			flagMap[f.Name] = f
		})
	}
}

func update(name string, value string) {
	if flagMap == nil {
		return
	}

	if f, ok := flagMap[name]; ok {
		f.Value.Set(value)
		log.Printf("update %s to %s", name, value)
		return
	}

	log.Printf("no parameter %s, value %s", name, value)
}
