package conf

import (
	"flag"
	"fmt"
	"testing"
	"time"
)

var (
	a = flag.Int("a", 10, "usage")
	b = flag.String("b", "bbb", "usage")
	c = flag.Bool("c", false, "usage")
)

func Test(t *testing.T) {
	flag.Parse()
	InitFlag()

	printFlag()
	time.Sleep(1 * time.Second)

	update("a", "1010")
	update("b", "lalala")
	update("c", "true")

	printFlag()
}
