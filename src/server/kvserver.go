package main

import (
	"paxos"
	"time"
	"sync"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	INSERT = 1
	DELETE = 2
	GET    = 3
	UPDATE = 4
)

var server *Server = nil

type Op struct {
	Operation      int
	Key            string
	Value          string
	Owner          int
	Valid          bool
}

type Item struct {
	SequenceNumber int
	Op             *Op

	Next           *Item
}

type Server struct {
	peer      *paxos.Paxos
	tail      *Item
	me        int

	max_seq   int
	tem_num   int

	check_max_lock  *sync.Mutex
	// check_done_lock *sync.Mutex
	edit_list_lock  *sync.Mutex
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
		// server.check_done_lock = new(sync.Mutex)
		server.edit_list_lock = new(sync.Mutex)
	}
	return server
}

func newItem(seq int) *Item {
	item := new(Item)
	item.SequenceNumber = seq
	item.Op = new(Op)
	return item
}


func (self *Server) newOperation(op_code int, key string, value string) (bool, string) {
	// fmt.Println("New Operation with: ")
	// fmt.Println(strconv.Itoa(op_code) + ", " + key + ", " + value)

	op := new(Op)
	op.Operation = op_code
	op.Key = key
	op.Value = value
	op.Owner = self.me

	for true {
		seq := self.getSeq()

		self.peer.Start(seq, *op)

		decision := self.checkStatus(seq)

		item := self.addOp(seq, decision)

		if decision.Owner == self.me {
			flag, result := self.performOp(seq, decision)
			item.Op.Valid = flag
			return flag, result
		}

	}

	return false, ""
}

func (self *Server) checkStatus(seq int) Op {
	var status bool = false
	var op interface{}
	for true {
		fmt.Print("checking:")
		fmt.Println(seq)
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
	rtn_val := self.peer.Max()
	if self.peer.Max() == -1 {
		rtn_val = 1
	} else {
		rtn_val = rtn_val + 1
	}
	self.check_max_lock.Unlock()

	return rtn_val
}

func (self *Server) addOp(seq int, op Op) *Item {
	self.edit_list_lock.Lock()
	defer self.edit_list_lock.Unlock()

	fmt.Println("New Op is added:")
	fmt.Println(strconv.Itoa(seq) + ", " + strconv.Itoa(op.Operation) + ", " + op.Key + ", " + op.Value + ", " + strconv.Itoa(op.Owner))

	// self.check_done_lock.Lock()
	if seq > self.max_seq {
		self.max_seq = seq
	}
	self.tem_num = self.tem_num + 1
	if self.tem_num == self.max_seq {
		self.peer.Done(self.max_seq)
	}
	// self.check_done_lock.Unlock()

	var pre_pos *Item = nil
	tem_pos := self.tail
	var new_item *Item = nil
	for true {
		if tem_pos.SequenceNumber < seq {
			new_item = newItem(seq)
			new_item.Op.Operation = op.Operation
			new_item.Op.Key = op.Key
			new_item.Op.Value = op.Value

			new_item.Next = tem_pos
			if pre_pos != nil {
				pre_pos.Next = new_item
			}
			if tem_pos == self.tail {
				self.tail = new_item
			}
			break
		} else if tem_pos.SequenceNumber > seq {
			if tem_pos.SequenceNumber == 0 {
				break
			}
			pre_pos = tem_pos
			tem_pos = tem_pos.Next
		} else {
			fmt.Println("Error! Try to Add an operation already exist!")
			break
		}
	}
	return new_item
}

func (self *Server) dump() []byte {
	if self.peer.Max() > self.tail.SequenceNumber {
		self.dealWithHole(self.peer.Max())
	}

	item_set := make([]Op, self.tem_num + 1, self.tem_num + 1)

	tem_pos := self.tail
	tem_cnt := 0

	for true {
		if tem_pos == nil {
			break
		}

		item_set[tem_cnt] = *tem_pos.Op

		tem_cnt++
		tem_pos = tem_pos.Next
	}

	rsp, err := json.Marshal(item_set)
	if err != nil {
		rsp = returnError()
	}
	return rsp
}

func (self *Server) dumpMap() []byte {
	m := self.getMap()
	data := make([][2]string, len(m))
	counter := 0
	for k, v := range m {
		data[counter] = [2]string{k, v}
		counter += 1
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		rsp = returnError()
	}
	return rsp
}

func (self *Server) countKey() []byte {
	m := self.getMap()
	data := struct {
		Result int `json:"result"`
	} {
		len(m),
	}
	rsp, err := json.Marshal(data)
	if err != nil {
		rsp = returnError()
	}
	return rsp
}

func (self *Server) getMap() map[string]string {
	m := make(map[string]string)
	forbidden_m := make(map[string]bool)

	if self.peer.Max() > self.tail.SequenceNumber {
		self.dealWithHole(self.peer.Max())
	}
	tem_pose := self.tail
	for true {
		if tem_pose == nil {
			break
		}
		switch tem_pose.Op.Operation {
		case UPDATE:
			_, ok := forbidden_m[tem_pose.Op.Key]
			if tem_pose.Op.Valid && !ok {
				m[tem_pose.Op.Key] = tem_pose.Op.Value
				forbidden_m[tem_pose.Op.Key] = true
			}
		case INSERT:
			_, ok := forbidden_m[tem_pose.Op.Key]
			if tem_pose.Op.Valid && !ok {
				m[tem_pose.Op.Key] = tem_pose.Op.Value
				forbidden_m[tem_pose.Op.Key] = true
			}
		case DELETE:
			if tem_pose.Op.Valid {
				forbidden_m[tem_pose.Op.Key] = true
			}
		case GET:
		}
		tem_pose = tem_pose.Next
	}
	return m
}
