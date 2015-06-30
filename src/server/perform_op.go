package main

import (
	// "strconv"
	// "fmt"
)

func (self *Server) performOp(seq int, op Op) (bool, string) {
	switch op.Operation {
	case GET: return self.performGET(seq, op)
	case INSERT: return self.performINSERT(seq, op)
	case UPDATE: return self.performUPDATE(seq, op)
	case DELETE: return self.performDELETE(seq, op)
	}
	return false, ""
}

func (self *Server) performGET(seq int, op Op) (bool, string) {
	list := []int {INSERT, UPDATE, DELETE}
	seq, item := self.moveToTarget(list, op.Key)
	if seq == 0 {
		// Try to get an non-existent key
		return false, ""
	}

	if seq == -1 {
		self.dealWithHole(item.SequenceNumber - 1)
		return self.performGET(seq, op)
	}

	switch item.Op.Operation {
	case INSERT, UPDATE:
		return true, item.Op.Value
	case DELETE:
		return false, ""
	}

	return false, ""
}

func (self *Server) performINSERT(seq int, op Op) (bool, string) {
	list := []int {INSERT, DELETE}
	seq, item := self.moveToTarget(list, op.Key)
	// fmt.Println("Move to:")
	// fmt.Println(strconv.Itoa(seq))
	if seq == 0 {
		return true, ""
	}

	if seq == -1 {
		self.dealWithHole(item.SequenceNumber - 1)
		return self.performINSERT(seq, op)
	}

	switch item.Op.Operation {
	case INSERT:
		return false, item.Op.Value
	case DELETE:
		return true, ""
	}

	return false, ""
}

func (self *Server) performUPDATE(seq int, op Op) (bool, string) {
	list := []int {INSERT, DELETE}
	seq, item := self.moveToTarget(list, op.Key)
	if seq == 0 {
		// Try to update an non-existent key
		return false, ""
	}

	if seq == -1 {
		self.dealWithHole(item.SequenceNumber - 1)
		return self.performUPDATE(seq, op)
	}

	switch item.Op.Operation {
	case INSERT:
		return true, ""
	case DELETE:
		return false, ""
	}

	return false, ""
}

func (self *Server) performDELETE(seq int, op Op) (bool, string) {
	list := []int {INSERT, DELETE}
	seq, item := self.moveToTarget(list, op.Key)
	if seq == 0 {
		// Try to delete an non-existent key
		return false, ""
	}

	if seq == -1 {
		self.dealWithHole(item.SequenceNumber - 1)
		return self.performDELETE(seq, op)
	}

	switch item.Op.Operation {
	case INSERT:
		return true, ""
	case DELETE:
		return false, ""
	}

	return false, ""
}

func (self *Server) moveToTarget(op_list []int, key string) (int, *Item) {
	var pre_pos *Item = self.tail
	var tem_pos *Item = self.tail.Next
	for true {
		// fmt.Print("Tempos Number:")
		// fmt.Println(tem_pos.SequenceNumber)
		// fmt.Print("Prepos Number:")
		// fmt.Println(pre_pos.SequenceNumber)
		if pre_pos != nil && tem_pos.SequenceNumber != pre_pos.SequenceNumber - 1 {
			return -1, pre_pos
		}
		if tem_pos.SequenceNumber == 0 {
			return 0, nil
		}

		flag := false
		for _, op := range op_list {
			if tem_pos.Op.Operation == op && tem_pos.Op.Key == key && tem_pos.Op.Valid {
				flag = true
				break
			}
		}
		if flag {
			break
		}

		pre_pos = tem_pos
		tem_pos = tem_pos.Next
	}

	return tem_pos.SequenceNumber, tem_pos
}

func (self *Server) dealWithHole(seq int) {
	// fmt.Println("Hole in " + strconv.Itoa(seq))
	op := self.checkStatus(seq)
	item := self.addOp(seq, op)
	flag, _ := self.performOp(seq, op)
	item.Op.Valid = flag
}
