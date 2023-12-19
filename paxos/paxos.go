package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"


type Paxos struct {
  mu         sync.Mutex
  l          net.Listener
  dead       bool
  unreliable bool
  rpcCount   int
  peers      []string
  me         int // index for peers

  instances map[int]*Instance
  done []int //highest sequence number associated with each peer
}

type Instance struct {
	Decided  string
	Va interface{}
	Np int64
	Na int64
}

type PrepareArgs struct {
  Seq int
  N int64
}

type PrepareReply struct {
  Err string
  Value interface{}
  N int64
}

type AcceptArgs struct {
  Seq int
  Value interface{}
  N int64
}

type AcceptReply struct {
  Err string
}

type DecideArgs struct {
  Seq int
  Value interface{}
  ServerIndex int
  Done int
  N int64
}

type DecideReply struct {
  Err string
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) AcceptorPrepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  inst, ok := px.instances[args.Seq]
  if ok {
    if inst.Np > args.N { //check if Np associated with this peer is less than P
		  reply.Err = "Rejected"
    } else {
      inst.Np = args.N //Np = N
      reply.Err = "OK"
    }
  } else {
    inst = &Instance{}
    inst.Decided = "No"
    inst.Va = nil //has not been accepted
    inst.Np = args.N
    inst.Na = 0 //has not been accepted
    px.instances[args.Seq] = inst
	  reply.Err = "OK"
  }

  reply.Value = inst.Va //Va
  reply.N = inst.Na //Na
  return nil
}

func (px *Paxos) AcceptorAccept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  inst, ok := px.instances[args.Seq]
  if ok {
    if inst.Np > args.N {
		reply.Err = "Rejected"
    } else {
	  inst.Va = args.Value //Va = V
      inst.Na = args.N //Na = N
      inst.Np = args.N //Np = N
      reply.Err = "OK"
    }
  } else {
    inst = &Instance{}
    inst.Decided = "No"
    inst.Va = args.Value
    inst.Np = args.N
    inst.Na = args.N
    px.instances[args.Seq] = inst
    reply.Err = "OK"
  }
  return nil
}

func (px *Paxos) AcceptorDecide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  inst, ok := px.instances[args.Seq]
  if ok {
    inst.Decided = "Yes"
    inst.Va = args.Value
    inst.Np = args.N
    inst.Na = args.N
    reply.Err = "OK"
  } else {
    inst = &Instance{"Yes", args.Value, 0, 0} //do not care about Np and Na
    px.instances[args.Seq] = inst
  }
  px.done[args.ServerIndex] = args.Done
  reply.Err = "OK"
  return nil
}

func (px *Paxos) ProposerPrepare(seq int, v interface{}) (bool, int64, interface{}) {
  var wg sync.WaitGroup 
  N := time.Now().UnixNano()
  agreed := 0
  Np := int64(0)
  Va := interface{}(nil)
  wg.Add(len(px.peers))

  for i, p := range px.peers {
    go func(i int, host string) {
      defer wg.Done()
      reply := PrepareReply{}
      ok := false
      if i == px.me {
        px.AcceptorPrepare(&PrepareArgs{seq, N}, &reply)
        if reply.Err == "OK" {
          ok = true
        }
      } else {
        ok = call(host, "Paxos.AcceptorPrepare", &PrepareArgs{seq, N}, &reply)
        if ok && reply.Err == "OK" {
          ok = true
        } else {
          ok = false
        }
      }

      px.mu.Lock() // Lock around shared data
      defer px.mu.Unlock()

      if ok {
        agreed++
        if reply.N > Np {
          Va = reply.Value
          Np = reply.N
        }
      }
    }(i, p)
  }

  wg.Wait()
  if agreed > len(px.peers)/2 {
    return true, N, Va
  }
  return false, N, Va
}

func (px *Paxos) ProposerAccept(seq int, v interface{}, N int64) (bool, int64) {
  agreed := 0
  var wg sync.WaitGroup
  wg.Add(len(px.peers))
  for i, peer := range px.peers {
    go func(i int, peer string) {
      defer wg.Done()
      reply := AcceptReply{}
      ok := false
      if i == px.me {
        px.AcceptorAccept(&AcceptArgs{seq, v, N}, &reply)
        if reply.Err == "OK" {
          ok = true
        }
      } else {
        ok = call(peer, "Paxos.AcceptorAccept", &AcceptArgs{seq, v, N}, &reply)
        if ok && reply.Err == "OK" {
          ok = true
        } else {
          ok = false
        }
      }
      px.mu.Lock() // Lock around shared data
      defer px.mu.Unlock()
      if ok {
        agreed++
      }
    }(i, peer)
  }
  wg.Wait()
  if agreed > len(px.peers)/2 {
    return true, N
  }
  return false, N
}

func (px *Paxos) ProposerDecide(seq int, v interface{}, N int64) bool {
  var wg sync.WaitGroup
  wg.Add(len(px.peers))
  agreed := 0
  for i, peer := range px.peers {
    go func(i int, peer string) {
      defer wg.Done()
      reply := DecideReply{}
      ok := false
      if i == px.me {
        px.AcceptorDecide(&DecideArgs{seq, v, px.me, px.done[px.me], N}, &reply)
        if reply.Err == "OK" {
          ok = true
        }
      } else {
        ok = call(peer, "Paxos.AcceptorDecide", &DecideArgs{seq, v, px.me, px.done[px.me], N}, &reply)
        if ok && reply.Err == "OK" {
          ok = true
        } else {
          ok = false
        }
      }
      px.mu.Lock() // Lock around shared data
      defer px.mu.Unlock()
      if ok {
        agreed++
      }
    }(i, peer)
  }
  wg.Wait()
  if agreed > len(px.peers)/2 {
    return true
  }
  return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  minSeq := px.Min()
  if seq < minSeq {
   return
  }
  go func() {
    for {
      if px.dead {
        return
      }
      ok, Na, Va := px.ProposerPrepare(seq, v)
      for !ok {
        if px.dead {
          return
        }
        time.Sleep(20 * time.Millisecond)
        ok, Na, Va = px.ProposerPrepare(seq, v)
      }
      if Va != nil {
          v = Va
      }
      ok, N := px.ProposerAccept(seq, v, Na)
      if !ok {
        time.Sleep(20 * time.Millisecond)
        continue
      } else {
        time.Sleep(20 * time.Millisecond)
      }
      for !px.ProposerDecide(seq, v, N) {
        if px.dead {
          return
        }
        time.Sleep(20 * time.Millisecond)
        px.ProposerDecide(seq, v, N)
      }
      break
    }
  }()
 }

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  if seq >= px.done[px.me] {
    px.done[px.me] = seq
  }
  // Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  res := 0

  for seq, _ := range px.instances {
    if seq > res {
      res = seq
    }
  }
  return res
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  res := 99999999
  if len(px.peers) == 0 {
    res = 0
  } else {
    for _, seq := range px.done {
      if res > seq {
        res = seq
      }
    }
  }
  for seq, inst := range px.instances {
    if seq < res {
      if inst.Decided == "Yes" {
        delete(px.instances, seq)
      }
    }
  }
  return res + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  inst, ok := px.instances[seq]
  if ok {
    if inst.Decided == "Yes" {
      return true, inst.Va
    }
  }
  return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.done = make([]int, len(peers))
  px.instances = make(map[int]*Instance)
  for i := 0; i < len(peers); i++ {
    px.done[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me])
    if e != nil {
      log.Fatal("listen error: ", e)
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63()%1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63()%1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}