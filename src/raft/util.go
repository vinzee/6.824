package raft

import (
	"log"
	"os"
)

// Debugging
var Debug bool = false

const (
	Reset  string = "\033[0m"
	Red           = "\033[31m"
	Green         = "\033[32m"
	Yellow        = "\033[33m"
	Blue          = "\033[34m"
	Purple        = "\033[35m"
	Cyan          = "\033[36m"
	White         = "\033[37m"
)

func SetDebugMode() {
	if os.Getenv("DEBUG") == "true" {
		Debug = true
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func PrintfError(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(Red+format+Reset, a...)
	}
	return
}

func PrintfSuccess(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(Green+format+Reset, a...)
	}
	return
}

func PrintfInfo(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(Blue+format+Reset, a...)
	}
	return
}

func PrintfDebug(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(White+format+Reset, a...)
	}
	return
}

func PrintfWarn(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(Yellow+format+Reset, a...)
	}
	return
}

func PrintfPurple(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(Purple+format+Reset, a...)
	}
	return
}

func PrintfCyan(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(Cyan+format+Reset, a...)
	}
	return
}
