package utils

import (
	"hash/fnv"
)

// Compute hash of value
func ComputeHash(value string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(value))
	hashValue := h.Sum32()
	// return int32(hashValue)
	return uint32(hashValue)
}

type FileSystemMessage struct {
	Action          string // "create", "append", etc
	Filename        string
	AppendTimestamp int64
	Content         []byte
}

type FileSystemReply struct {
	Content []byte
	Error   string
}

func Mod(a, b int) int {
	return (a%b + b) % b
}
