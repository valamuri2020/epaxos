package dlog

import stdlog "log"

const DLOG = false

func Printf(format string, v ...interface{}) {
	if !DLOG {
		return
	}
	stdlog.Printf(format, v...)
}

func Println(v ...interface{}) {
	if !DLOG {
		return
	}
	stdlog.Println(v...)
}
