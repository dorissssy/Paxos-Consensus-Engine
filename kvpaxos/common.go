package kvpaxos

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
// type Err string

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  ClientID string
}

type PutReply struct {
  Err string
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  ClientID string
}

type GetReply struct {
  Err string
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}