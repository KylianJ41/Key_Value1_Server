package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu              sync.Mutex
	data            map[string]string
	lastApplied     map[int64]int64
	lastAppliedData map[int64]string
}

// checkDuplicate checks if the request is a duplicate and returns the last known result if it is.
// It returns true if the request is a duplicate, false otherwise.
func (kv *KVServer) checkDuplicate(clientId int64, seqNum int64) (bool, string) {
	if lastSeq, ok := kv.lastApplied[clientId]; ok && seqNum <= lastSeq {
		return true, kv.lastAppliedData[clientId]
	}
	return false, ""
}

// updateLastApplied updates the last applied sequence number and result for a client.
func (kv *KVServer) updateLastApplied(clientId int64, seqNum int64, result string) {
	kv.lastApplied[clientId] = seqNum
	kv.lastAppliedData[clientId] = result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if isDuplicate, lastResult := kv.checkDuplicate(args.ClientId, args.SeqNum); isDuplicate {
		reply.Value = lastResult
		return
	}

	value, exists := kv.data[args.Key]
	if exists {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	kv.updateLastApplied(args.ClientId, args.SeqNum, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if isDuplicate, lastResult := kv.checkDuplicate(args.ClientId, args.SeqNum); isDuplicate {
		reply.Value = lastResult
		return
	}

	kv.data[args.Key] = args.Value
	kv.updateLastApplied(args.ClientId, args.SeqNum, "")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if isDuplicate, lastResult := kv.checkDuplicate(args.ClientId, args.SeqNum); isDuplicate {
		reply.Value = lastResult
		return
	}

	oldValue, exists := kv.data[args.Key]
	if exists {
		kv.data[args.Key] = oldValue + args.Value
	} else {
		kv.data[args.Key] = args.Value
	}
	reply.Value = oldValue

	kv.updateLastApplied(args.ClientId, args.SeqNum, oldValue)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.lastAppliedData = make(map[int64]string)

	return kv
}
