package util

import "time"

func GetTimeInMilliSecond() int64 {
	return time.Now().UnixNano() / 1e6
}
