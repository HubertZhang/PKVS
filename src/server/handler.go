package main

import (
	"net/http"
	"encoding/json"
	"os"
)

func handleGet(w http.ResponseWriter, r *http.Request) {
	k := r.URL.Query().Get("key")
	var success, value = server.newOperation(GET, k, "")
	data := struct {
		Success bool `json:"success"`
		Value string `json:"value"`
	} {
		success,
		value,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		rsp = returnError()
	}
	writeResponse(rsp, w)
}


func handleInsert(w http.ResponseWriter, r *http.Request) {
	k := r.PostFormValue("key")
	v := r.PostFormValue("value")
	var success, _ = server.newOperation(INSERT, k, v)
	data := struct {
		Success bool `json:"success"`
	} {
		success,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		rsp = returnError()
	}
	writeResponse(rsp, w)
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	k := r.PostFormValue("key")
	v := r.PostFormValue("value")
	var success, _ = server.newOperation(UPDATE, k, v)
	data := struct {
		Success bool `json:"success"`
	} {
		success,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		rsp = returnError()
	}
	writeResponse(rsp, w)

}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	k := r.PostFormValue("key")
	var success, _ = server.newOperation(DELETE, k, "")
	data := struct {
		Success bool `json:"success"`
	} {
		success,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		rsp = returnError()
	}
	writeResponse(rsp, w)
}

func handleDump(w http.ResponseWriter, r *http.Request) {
	writeResponse(server.dumpMap(), w)
}

func handleCount(w http.ResponseWriter, r *http.Request) {
	writeResponse(server.countKey(), w)
}

func handleHalt(w http.ResponseWriter, r *http.Request) {
	server.peer.Kill()
	os.Exit(0)
}

func handleDumpLog(w http.ResponseWriter, r *http.Request) {
	writeResponse(server.dump(), w)
}

func returnError() []byte {
	data := struct {
		Success bool `json:"success"`
	} {
		false,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	return rsp
}


func writeResponse(content []byte, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(content)
}
