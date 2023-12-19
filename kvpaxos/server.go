package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  
  Key string
  Value string
  ClientID string
  DoHash bool
  Operation string
}

type KVPaxos struct {
  mu         sync.Mutex
  l          net.Listener
  me         int
  dead       bool // for testing
  unreliable bool // for testing
  px         *paxos.Paxos

  // Your definitions here.
  maxSeq int
  dataStore  map[string]string
  clientIds map[string]string
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := &Op{}
  op.Key = args.Key
  op.ClientID = args.ClientID
  op.Operation = "Get"

  err, value := kv.agreeValueAndExecOperation(*op)
  reply.Err = err
  reply.Value = value

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  op := &Op{}
  op.Key = args.Key
  op.Value = args.Value
  op.DoHash = args.DoHash
  op.ClientID = args.ClientID
  op.Operation = "Put"

  err, value := kv.agreeValueAndExecOperation(*op)
  reply.Err = err
  reply.PreviousValue = value

  return nil
}

func (kv *KVPaxos) agreeValueAndExecOperation(op Op) (string, string) {
  kv.maxSeq++
  //This loop ensures that the operation op is proposed and agreed upon by the system
  //If the operation is not agreed upon, the system retries with the next seq
  kv.px.Start(kv.maxSeq, op)
  // agreed value
  Va := kv.agreeValue(kv.maxSeq)
  for Va != op {
    // for each seq, fetch the agreed upon value and apply the operations
    v := kv.agreeValue(kv.maxSeq) //keep up with missed seq num
    kv.execOperation(v)

    kv.maxSeq++
    kv.px.Start(kv.maxSeq, op)
    Va = kv.agreeValue(kv.maxSeq)
  }

  err, value := kv.execOperation(op)

  kv.px.Done(kv.maxSeq)
  return err, value
}
//get the accepted agareed value
func (kv *KVPaxos) agreeValue(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, v := kv.px.Status(seq)
    if decided {
      return v.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

func (kv *KVPaxos) execOperation(op Op) (string, string) {
  if op.Operation == "Get" {
    value, ok := kv.dataStore[op.Key]
    if ok {
      return OK, value
    } else {
      return ErrNoKey, ""
    }
  } else if op.Operation == "Put" {
    prevValue, ok := kv.clientIds[op.ClientID]
    // return if clientid already exists
    if !ok{
        prevValue = kv.dataStore[op.Key]
        if op.DoHash {
            kv.dataStore[op.Key] = strconv.Itoa(int(hash(prevValue + op.Value)))
            kv.clientIds[op.ClientID] = prevValue
        } else {
            kv.dataStore[op.Key] = op.Value
            prevValue = "" //for releasing memory
            kv.clientIds[op.ClientID] = ""
        }
    }
    return OK, prevValue
  }
  return "Invalid", ""

}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.dataStore = make(map[string]string)
  kv.clientIds = make(map[string]string)
  kv.maxSeq = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63()%1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63()%1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}