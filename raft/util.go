package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func LookUpEleInSlice(src []int, key int) int {
	for index, val := range src {
		if val == key {
			return index
		}
	}
	return -1
}
