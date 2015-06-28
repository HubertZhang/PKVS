package main

import (
	"net/http"
)

func main () {
	http.HandleFunc("/kv/insert", handleInsert)
	http.HandleFunc("/kv/get", handleGet)
	http.HandleFunc("/kv/delete", handleDelete)
	http.HandleFunc("/kv/update", handleUpdate)

	http.HandleFunc("/kvman/dump", handleDump)
	http.HandleFunc("/kvman/count", handleCount)
	http.HandleFunc("/kvman/halt", handleHalt)

	http.ListenAndServe(":8080", nil)
}
