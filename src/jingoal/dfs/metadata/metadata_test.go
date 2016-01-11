package metadata

import (
	"log"
	"testing"
)

func TestFindPerfectSegment(t *testing.T) {
	segments := []*Segment{&Segment{Domain: 1, NormalServer: "shard1"}, &Segment{Domain: 2, NormalServer: "shard2"}, &Segment{Domain: 3, NormalServer: "shard1"}, &Segment{Domain: 5, NormalServer: "shard2"}}

	var seg *Segment
	seg = FindPerfectSegment(segments, 1)
	log.Println(seg)

	seg = FindPerfectSegment(segments, 4)
	log.Println(seg)

	seg = FindPerfectSegment(segments, 5)
	log.Println(seg)

	seg = FindPerfectSegment(segments, 7)
	log.Println(seg)
}
