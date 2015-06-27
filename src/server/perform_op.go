package main

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
	return false, ""
}

func (self *Server) performINSERT(seq int, op Op) (bool, string) {
	return false, ""
}

func (self *Server) performUPDATE(seq int, op Op) (bool, string) {
	return false, ""
}

func (self *Server) performDELETE(seq int, op Op) (bool, string) {
	return false, ""
}

func (self *Server) moveToTarget(op_list []int) int {
	return 0
}
