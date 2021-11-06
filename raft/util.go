package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if os.Getenv("DEBUG") == "1" {
		log.Printf(format, a...)
	}
	return
}
