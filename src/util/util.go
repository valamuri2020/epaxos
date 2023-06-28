package util

import (
	"time"
)

// const CHAN_BUFFER_SIZE = 200000
// var counter int64

func MakeTimestamp(offset int64) int64 {
	return time.Now().UnixNano()/int64(time.Millisecond) + offset
}

func MakeTimestampNoOff() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// func GetCounter() int64 {
// 	return atomic.AddInt64(&counter, 1) - 1
// }
