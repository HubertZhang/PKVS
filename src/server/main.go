package main

import (
	"net/http"
	"paxos"
	"fmt"
	"os"
	"strconv"
	"encoding/gob"
)
var paxos_peer *paxos.Paxos

func main () {
	gob.Register(Op{})
	if len(os.Args) != 2 {
		fmt.Print("Usage: "+os.Args[0] +" <node id>")
		return
	}
	me, _ := strconv.Atoi(os.Args[1])
	peers, port, err := load_config()
	if err != nil {
		fmt.Print(err)
		return
	}
//	peers[me-1] = "10000"

	paxos_peer = paxos.Make(peers, me-1, nil)
	getServer(paxos_peer, me-1)
	fmt.Println("here")
	http.HandleFunc("/kv/insert", handleInsert)
	http.HandleFunc("/kv/get", handleGet)
	http.HandleFunc("/kv/delete", handleDelete)
	http.HandleFunc("/kv/update", handleUpdate)

	http.HandleFunc("/kvman/dump", handleDump)
	http.HandleFunc("/kvman/count", handleCount)
	http.HandleFunc("/kvman/halt", handleHalt)
	fmt.Println(":"+strconv.Itoa(port))

	err = http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		fmt.Println(err)
	}
}
