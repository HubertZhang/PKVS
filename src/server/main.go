package main

import (
	"net/http"
	"paxos"
	"fmt"
	"os"
	"strconv"
	"encoding/gob"
	"strings"
)
var paxos_peer *paxos.Paxos

func main () {
	gob.Register(Op{})
	if len(os.Args) != 2 {
		fmt.Print("Usage: "+os.Args[0] +" <node id>")
		return
	}
	me, _ := strconv.Atoi(os.Args[1])
	peers, default_port, local_ports, err := load_config()
	if err != nil {
		fmt.Print(err)
		return
	}
	if peers == nil {
		return
	}

	peers[me-1] =":"+ strings.Split(peers[me-1], ":")[1]
	fmt.Println("Paxos port" + peers[me-1])
//	peers[me-1] = "10000"

	paxos_peer = paxos.Make(peers, me-1, nil)
	getServer(paxos_peer, me-1)

	http.HandleFunc("/kv/insert", handleInsert)
	http.HandleFunc("/kv/get", handleGet)
	http.HandleFunc("/kv/delete", handleDelete)
	http.HandleFunc("/kv/update", handleUpdate)

	http.HandleFunc("/kvman/dump", handleDump)
	http.HandleFunc("/kvman/countkey", handleCount)
	http.HandleFunc("/kvman/shutdown", handleHalt)
	http.HandleFunc("/kvman/dumplog", handleDumpLog)

	var port int
	if default_port == 0 {
		port = local_ports[me-1]
	} else {
		port = default_port
	}
	fmt.Print("Peers Number:")
	fmt.Println(len(peers))
	fmt.Print("Ports Number:")
	fmt.Println(port)

	err = http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		fmt.Println(err)
	}
}
