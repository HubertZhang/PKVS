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

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	npaxos int
	next   int
	tail   *LogEntry
	done   int
	dones  []int
}

type LogEntry struct {
	// mu      sync.Mutex
	decided bool
	num     int
	prev    *LogEntry
	// val     interface{}

	// seq-num & val
	n_p int
	n_a int
	v_a interface{}
}

type NumVal struct {
	Decided bool
	Seq     int
	N       int
	Val     interface{}
	Me      int
	Done    int
	Replied bool
}

func makeLogEntry(seq int) *LogEntry {
	ret := new(LogEntry)

	ret.decided = false
	ret.num = seq
	ret.prev = nil

	ret.n_p = 0
	ret.n_a = 0

	return ret
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

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//

// get without create
func (px *Paxos) lock() {
	// fmt.Printf("%d locking\n", px.me)
	px.mu.Lock()
}

func (px *Paxos) unlock() {
	// fmt.Printf("%d unlocking\n", px.me)
	px.mu.Unlock()
}

func (px *Paxos) getLog(seq int) *LogEntry {
	var ret *LogEntry
	// only read OPs
	// no need to muteix
	if seq >= px.next {
		return nil
	} else {
		for ret = px.tail; ret != nil && ret.num != seq; ret = ret.prev {
			// do nothing
		}
		return ret
	}
}

// get with create
func (px *Paxos) extLog(seq int) *LogEntry {
	var ret *LogEntry
	if seq >= px.next {
		for i := px.next; i <= seq; i++ {
			iter := makeLogEntry(i)
			iter.prev = px.tail
			px.tail = iter
		}
		px.next = px.tail.num + 1
		ret = px.tail
	} else {
		for ret = px.tail; ret != nil && ret.num != seq; ret = ret.prev {
			// do nothing
		}
	}
	return ret
}

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.lock()
	defer px.unlock()

	if seq < px.min() {
		return
	}

	// insert new instance into log-list
	log := px.extLog(seq)

	// if already decided
	if log.decided {
		return
	}

	go px.Propose(seq, v)
}

func (px *Paxos) updateDones(peerNum int, doneVal int) {
	if doneVal > px.dones[peerNum] {
		px.dones[peerNum] = doneVal
	}
}

func (px *Paxos) Prepare(msg *NumVal, reply *NumVal) error {
	px.lock()
	defer px.unlock()

	// fmt.Printf("%d prepare-ack\n", px.me)

	log := px.extLog(msg.Seq)

	if log.decided {
		*reply = NumVal{true, msg.Seq, log.n_a, log.v_a, px.me, px.done, true}

		return nil
	}
	if msg.N > log.n_p {
		log.n_p = msg.N

		px.updateDones(msg.Me, msg.Done)

		*reply = NumVal{false, msg.Seq, log.n_a, log.v_a, px.me, px.done, true}
	} else {
		*reply = NumVal{false, -1, -1, nil, px.me, px.done, true}
	}

	return nil
}

func (px *Paxos) Accept(msg *NumVal, reply *NumVal) error {
	px.lock()
	defer px.unlock()

	// fmt.Println("accept-ack")

	log := px.extLog(msg.Seq)

	if log.decided {
		*reply = NumVal{true, msg.Seq, log.n_a, log.v_a, px.me, px.done, true}

		return nil
	}
	if msg.N >= log.n_p {
		log.n_p = msg.N
		log.n_a = msg.N
		log.v_a = msg.Val

		px.updateDones(msg.Me, msg.Done)

		*reply = NumVal{false, msg.Seq, log.n_a, log.v_a, px.me, px.done, true}
	} else {
		*reply = NumVal{false, -1, -1, nil, px.me, px.done, true}
	}

	return nil
}

func (px *Paxos) decide(msg *NumVal) {
	// fmt.Printf("%d decide\n", px.me)

	log := px.extLog(msg.Seq)

	if !log.decided {
		log.n_p = msg.N
		log.n_a = msg.N
		log.v_a = msg.Val
		log.decided = true
	}
	px.updateDones(msg.Me, msg.Done)

	// fmt.Printf("%d: ", px.me)
	// fmt.Println(log.v_a)
}

func (px *Paxos) Decide(msg *NumVal, reply *bool) error {
	px.lock()
	defer px.unlock()

	px.decide(msg)

	return nil
}

func (px *Paxos) proposeOneRound(seq int, v interface{}, ballot int) bool {
	// choose higher ballot number
	var val interface{} = v

	// fmt.Printf("%d proposing round\n", px.me)
	// prepare
	count := 0
	n := -1
	prepareMsg := NumVal{false, seq, ballot, v, px.me, px.done, true}
	for i := 0; i < px.npaxos; i++ {
		client, err := rpc.Dial("unix", px.peers[i])
		if err != nil {
			continue
		}
		var reply NumVal
		reply.Replied = false

		err = client.Call("Paxos.Prepare", &prepareMsg, &reply)
		client.Close()
		if !reply.Replied {
			continue
		}

		px.lock()
		px.updateDones(reply.Me, reply.Done)
		px.unlock()

		if reply.Seq == -1 {
			// rejected
			continue
		}
		count++
		if reply.N > n {
			n = reply.N
			if reply.Val == nil {
				val = v
			} else {
				val = reply.Val
			}
		}
	}

	// not majority
	if count <= px.npaxos/2 {
		return false
	}

	accept := NumVal{false, seq, ballot, val, px.me, px.done, true}
	count = 0
	for i := 0; i < px.npaxos; i++ {
		client, err := rpc.Dial("unix", px.peers[i])
		if err != nil {
			continue
		}
		var reply NumVal
		reply.Replied = false

		err = client.Call("Paxos.Accept", &accept, &reply)
		client.Close()
		if !reply.Replied {
			continue
		}

		px.lock()
		px.updateDones(reply.Me, reply.Done)
		px.unlock()

		if reply.Seq != -1 {
			count++
		}
	}

	// decided
	if count > px.npaxos/2 {
		for i := 0; i < px.npaxos; i++ {
			if i == px.me {
				continue
			}
			client, err := rpc.Dial("unix", px.peers[i])
			if err != nil {
				continue
			}
			var reply bool
			client.Call("Paxos.Decide", &accept, &reply)
			client.Close()
		}
		px.Decide(&accept, nil)
		return true
	} else {
		return false
	}
}

func (px *Paxos) Propose(seq int, v interface{}) {
	ballot := px.me + 1
	count := 0
	for true {
		count = count + 1
		if px.proposeOneRound(seq, v, ballot) {
			break
		}
		ballot = ballot + px.npaxos
	}
	// fmt.Printf("%d: %d times\n", px.me, count)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.lock()
	defer px.unlock()

	if seq <= px.done {
		return
	}
	flag := true
	for iter := px.tail; iter != nil; iter = iter.prev {
		if iter.num <= seq && !iter.decided {
			flag = false
			break
		}
	}
	if flag {
		px.done = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.lock()
	defer px.unlock()

	return px.next - 1
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
func (px *Paxos) min() int {
	// You code here.

	// find min-val among dones []int
	// GCing
	ret := px.dones[0]
	for i := 1; i < px.npaxos; i++ {
		if px.dones[i] < ret {
			ret = px.dones[i]
		}
	}

	return ret
}

func (px *Paxos) Min() int {
	// You code here.

	// find min-val among dones []int
	px.lock()
	defer px.unlock()

	ret := px.min() + 1

	// GCing
	var head *LogEntry
	for head = px.tail; head != nil && head.num != ret; head = head.prev {
		// do nothing
	}
	if head != nil {
		head.prev = nil
	}

	return ret
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
	px.lock()
	defer px.unlock()

	if seq >= px.next {
		return false, nil
	} else {
		// already GC-ed
		if seq < px.min() {
			return false, nil
		}

		log := px.getLog(seq)

		// log-entry already GC-ed
		return log.decided, log.v_a
	}
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

	// Your initialization code here.
	px.npaxos = len(peers)
	px.mu = sync.Mutex{}
	px.next = 0
	px.tail = nil
	px.done = -1
	px.dones = make([]int, px.npaxos)
	for i := 0; i < px.npaxos; i++ {
		px.dones[i] = -1
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
