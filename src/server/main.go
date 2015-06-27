package main

import (
	"net/http"
)

func main () {
	http.HandleFunc("/kv/insert", handleInsert)
	http.HandleFunc("/kv/get", handleGet)
	http.HandleFunc("/kv/delete", handleDelete)
	http.HandleFunc("/kv/update", handleUpdate)

	http.ListenAndServe(":8080", nil)
}
