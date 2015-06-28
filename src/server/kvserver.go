package main

import (
	"paxos"
	"time"
	"sync"
)

const (
	INSERT = 0
	DELETE = 1
	GET    = 2
	UPDATE = 3
)

var server *Server = nil

type Op struct {
	Operation      int
	Key            string
	Value          string
	Owner          int
}

type Item struct {
	SequenceNumber int
	Op             Op

	Next           *Item
}

type Server struct {
	peer      *paxos.Paxos
	tail      *Item
	me        int

	max_seq   int
	tem_num   int

	check_max_lock  *sync.Mutex
	check_done_lock *sync.Mutex
}

func getServer(paxos *paxos.Paxos, me int) *Server {
	if server == nil {
		server = new(Server)
		server.peer = paxos
		server.tail = newItem(0)
		server.me = me
		server.max_seq = 0
		server.tem_num = 0
		server.check_max_lock = new(sync.Mutex)
		server.check_done_lock = new(sync.Mutex)
	}
	return server
}

func newItem(seq int) *Item {
	item := new(Item)
	item.SequenceNumber = seq
	return item
}


func (self *Server) newOperation(op_code int, key string, value string) (bool, string) {
	op := new(Op)
	op.Operation = op_code
	op.Key = key
	op.Value = value
	op.Owner = self.me

	for true {
		seq := self.getSeq()

		self.peer.Start(seq, op)

		decision := self.checkStatus(seq)

		self.addOp(seq, decision)

		if decision.Owner == self.me {
			return self.performOp(seq, decision)
		}

	}

	return false, ""
}

func (self *Server) checkStatus(seq int) Op {
	var status bool = false
	var op interface{}
	for true {
		status, op = self.peer.Status(seq)
		if status {
			break
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return op.(Op)
}

func (self *Server) getSeq() int {
	self.check_max_lock.Lock()
	rtn_val := self.peer.Max() + 1
	self.check_max_lock.Unlock()

	return rtn_val
}

func (self *Server) addOp(seq int, op Op) {
	self.check_done_lock.Lock()
	if seq > self.max_seq {
		self.max_seq = seq
	}
	self.tem_num = self.tem_num + 1
	if self.tem_num == self.max_seq {
		self.peer.Done(self.max_seq)
	}
	self.check_done_lock.Lock()

	var pre_pos *Item = nil
	tem_pos := self.tail
	for true {
		if tem_pos.SequenceNumber < seq {
			new_item := newItem(seq)
			new_item.Op = op
			new_item.Next = tem_pos

			pre_pos.Next = new_item

			break
		} else if tem_pos.SequenceNumber > seq {
			if tem_pos.SequenceNumber == 0 {
				break
			}
			pre_pos = tem_pos
			tem_pos = tem_pos.Next
		} else {
			break
		}
	}
}
