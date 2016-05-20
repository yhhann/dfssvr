package instrument

import (
	"math/rand"
	"testing"
	"time"
)

func Test(t *testing.T) {
	StartMetrics()
	for {
		v := (rand.NormFloat64() * 200) + 10
		SuccessDuration <- &Measurements{
			Name:  "Normal",
			Value: v,
		}

		FailedCounter <- &Measurements{
			Name:  "FailedMethod",
			Value: v,
		}

		InProcess <- &Measurements{
			Name:  "InProcessMethod",
			Value: 1,
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
