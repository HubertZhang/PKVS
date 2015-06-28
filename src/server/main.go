package main

import (
	"net/http"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
)

func main () {
	me := strconv.Atoi(os.args[4])
	addrs := os.args[1:3]
	paxos := Make(addrs, me, nil)
	getServer(paxos, me)
	http.HandleFunc("/kv/insert", handleInsert)
	http.HandleFunc("/kv/get", handleGet)
	http.HandleFunc("/kv/delete", handleDelete)
	http.HandleFunc("/kv/update", handleUpdate)

	http.ListenAndServe(":8080", nil)
}
