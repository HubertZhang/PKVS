package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"strconv"
)

func load_config() (servers []string, port []int, err error) {
	bytes, err := ioutil.ReadFile("./conf/settings.conf")
	if err != nil {
		fmt.Println("Error on opening settings.conf")
		return nil, nil, err
	}
	var f interface{}
	err = json.Unmarshal(bytes, &f);
	if err != nil {
		fmt.Println("Error when parsing settings.conf")
		return nil, nil, err
	}
	configs := f.(map[string]interface{})
	length := len(configs) / 2
	servers = make([]string, length)
	port = make([]int, length)
	for k, v := range configs {
		if strings.HasPrefix(k, "port") {
			node_id, _ := strconv.Atoi(k[4:])
			fmt.Println(node_id)
			port[node_id-1] = int(v.(float64))
			fmt.Println(port)
		} else if strings.HasPrefix(k, "n") {
			node_id, _ := strconv.Atoi(k[1:])
			fmt.Println(node_id)
			servers[node_id-1] = v.(string)
		} else {
			fmt.Println("Error when parsing settings.conf, unknown key: "+k)
			return nil, nil, err
		}
	}
	return
}
