package main

import (
	"net/http"
	"encoding/json"
)

func handleGet(w http.ResponseWriter, r *http.Request) {
	k := r.URL.Query().Get("key")
	data := struct {
		Key string
	} {
		k,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		return
	}
	writeResponse(rsp, w)
}


func handleInsert(w http.ResponseWriter, r *http.Request) {
	k := r.PostFormValue("key")
	v := r.PostFormValue("value")
	data := struct {
		Key string;
		Value string
	} {
		k,
		v,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		return
	}
	writeResponse(rsp, w)
}

func handleUpdate(w http.ResponseWriter, r *http.Request) {
	k := r.PostFormValue("key")
	v := r.PostFormValue("value")
	data := struct {
		Key string;
		Value string
	} {
		k,
		v,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		return
	}
	writeResponse(rsp, w)

}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	k := r.PostFormValue("key")
	v := r.PostFormValue("value")
	data := struct {
		Key string;
		Value string
	} {
		k,
		v,
	}

	rsp, err := json.Marshal(data)
	if err != nil {
		return
	}
	writeResponse(rsp, w)
}

func handleDump(w http.ResponseWriter, r *http.Request) {
}

func handleCount(w http.ResponseWriter, r *http.Request) {
}

func handleHalt(w http.ResponseWriter, r *http.Request) {
}

func writeResponse(content []byte, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(content)
}
