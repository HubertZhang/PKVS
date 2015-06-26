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
	next  int
	tail  *LogEntry
	done  int
	dones []int
}

type LogEntry struct {
	mu      sync.Mutex
	decided bool
	num     int
	prev    *LogEntry
	val     interface{}

	// seq-num & val
	n_p int
	n_a int
	v_a interface{}
}

type NumVal struct {
	seq  int
	n    int
	val  interface{}
	me   int
	done int
}

func makeLogEntry(seq int) *LogEntry {
	ret := new(LogEntry)

	ret.mu = sync.Mutex{}
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
func (px *Paxos) getLog(seq int) *LogEntry {
	var ret *LogEntry
	// only read OPs
	// no need to mutex
	if seq >= next {
		return nil
	} else {
		for ret = tail; ret != nil && ret.num != seq; ret = ret.prev {
			// do nothing
		}
		return ret
	}
}

// get with create
func (px *Paxos) extLog(seq int) *LogEntry {
	var ret *LogEntry
	mu.Lock()
	if seq >= next {
		for i := next; i <= seq; i++ {
			iter = makeLogEntry(i)
			iter.prev = tail
			tail = iter
		}
		next = tail.num + 1
		ret = tail
	} else {
		for ret = tail; ret != nil && ret.num != seq; ret = ret.prev {
			// do nothing
		}
	}
	mu.Unlock()
	return ret
}

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < Min() {
		return
	}

	// insert new instance into log-list
	log := extLog(seq)

	// if already decided
	if log.decided {
		return
	}

	go Propose(seq, v, log)
}

func (px *Paxos) updateDones(peerNum int, doneVal int) {
	if doneVal > dones[peerNum] {
		mu.Lock()
		dones[peerNum] = doneVal
		mu.Unlock()
	}
}

func (px *Paxos) Prepare(msg NumVal) NumVal {
	log := extLog(msg.seq)
	if msg.n > log.n_p {
		log.mu.Lock()
		log.n_p = n
		log.mu.Unlock()
		updateDones(msg.me, msg.done)
		return NumVal{seq, log.n_a, log.val, me, done}
	}
	return NumVal{-1, -1, nil, me, done}
}

func (px *Paxos) Accept(msg NumVal) (bool, int, int) {
	log := extLog(msg.seq)
	if msg.n >= log.n_p {
		log.mu.Lock()
		log.n_p = msg.n
		log.n_a = msg.n
		log.v_a = msg.val
		log.mu.UnlocK()
		updateDones(msg.me, msg.done)
		return true, me, done
	}
	return false, me, done
}

func (px *Paxos) Decide(msg NumVal) {
	log := extLog(msg.seq)
	log.mu.Lock()
	log.n_p = msg.n
	log.n_a = msg.n
	log.v_a = msg.val
	log.decided = true
	log.mu.Unlock()
	updateDones(msg.me, msg.done)
}

func (px *Paxos) Propose(seq int, v interface{}, entry *LogEntry) {
	ballot := 0
	for entry.decided == false {
		// choose higher ballot number
		ballot = ballot + 1
		var val interface{} = v

		// prepare
		count := 0
		n := -1
		prepareMsg := NumVal{seq, ballot, v, me, done}
		for i := 0; i < 3; i++ {
			client, err := rpc.Dial("tcp", px.peers[i])
			if err != nil {
				fmt.Println("Paxos.Propose dialing" + px.peers[i] + "error !")
				continue
			}
			var reply NumVal
			err = client.Call("Paxos.Prepare", &prepareMsg, &reply)
			if err != nil {
				fmt.Println("Paxos.Propose prepare" + px.peers[i] + "connection failure")
				continue
			}
			updateDones(reply.me, reply.done)
			if reply.seq == -1 {
				// rejected
				continue
			}
			count++
			if reply.ballot > n {
				n = reply.ballot
				if reply.val == nil {
					val = v
				} else {
					val = reply.val
				}
			}
		}

		// not majority
		if count < 2 {
			continue
		}

		accept := NumVal{seq, ballot, val, me, done}
		count = 0
		for i := 0; i < 3; i++ {
			client, err := rpc.Dial("tcp", px.peers[i])
			if err != nil {
				fmt.Println("Paxos.Accept dialing" + px.peers[i] + "error !")
				continue
			}
			var reply struct {
				accepted int
				me       int
				done     int
			}
			err = client.Call("Paxos.Accept", &accept, &reply)
			if err != nil {
				fmt.Println("Paxos.Propose accept" + px.peers[i] + "connection failure")
				continue
			}
			updateDones(reply.me, reply.done)
			if reply.accepted {
				count++
			}
		}

		// decided
		if count >= 2 {
			for i := 0; i < 3; i++ {
				client, err := rpc.Dial("tcp", px.peers[i])
				if err != nil {
					fmt.Println("Paxos.Decide dialing" + px.peers[i] + "error !")
					continue
				}
				err = client.Call("Paxos.Decide", &accept)
				if err != nil {
					fmt.Println("Paxos.Decide decide" + px.peers[i] + "connection failure")
					continue
				}
			}
		}
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if seq <= done {
		return
	}
	flag := true
	for iter := tail; iter != nil; iter = iter.prev {
		if iter.num <= seq && !iter.decided {
			flag = false
			break
		}
	}
	if flag {
		mu.Lock()
		done = seq
		mu.Unlock()
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return next
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

	// find min-val among dones []int
	var ret int
	if dones[0] < dones[1] {
		if dones[0] < dones[2] {
			ret = dones[0]
		} else {
			ret = dones[2]
		}
	} else {
		if dones[1] < dones[2] {
			ret = dones[1]
		} else {
			ret = dones[2]
		}
	}
	ret++

	// GCing
	mu.Lock()
	var head *LogEntry
	for head = tail; head.num != ret; head = head.prev {
		// do nothing
	}
	head.prev = nil
	mu.Unlock()

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
	if seq >= next {
		return false, nil
	} else {
		// already GC-ed
		if seq < Min() {
			return false, nil
		}
		log := getLog(seq)
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
	mu = sync.Mutex{}
	next = 0
	tail = nil
	done = -1
	dones = [3]int{-1, -1, -1}

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
