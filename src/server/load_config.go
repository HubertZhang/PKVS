package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"strconv"
)

func load_config() (servers []string, default_port int, local_ports []int, err error) {
	bytes, err := ioutil.ReadFile("./conf/settings.conf")
	if err != nil {
		fmt.Println("Error on opening settings.conf")
		return nil, 0, nil, err
	}
	var f interface{}
	err = json.Unmarshal(bytes, &f);
	if err != nil {
		fmt.Println("Error when parsing settings.conf")
		return nil, 0, nil, err
	}
	configs := f.(map[string]interface{})
	servers_map := make(map[int]string)
	ports_map := make(map[int]int)

	default_port = 0
	local_ports = nil
	for k, v := range configs {
		if k == "port" {
			default_port = int(v.(float64))
		} else if strings.HasPrefix(k, "lp") {
			node_id, _ := strconv.Atoi(k[2:])
			ports_map[node_id-1] = int(v.(float64))
		} else if strings.HasPrefix(k, "n") {
			node_id, _ := strconv.Atoi(k[1:])
			servers_map[node_id-1] = v.(string)
		} else {
			fmt.Println("Error when parsing settings.conf, unknown key: "+k)
			return
		}
	}
	servers = make([]string, len(servers_map))
	for i := 0; i< len(servers_map); i++ {
		value, exist := servers_map[i]
		if !exist {
			fmt.Println("Error when parsing settings.conf, missing server ip for n"+strconv.Itoa(i+1))
			return nil, 0, nil, nil
		}
		servers[i] = value
	}
	if default_port == 0 {
		local_ports = make([]int, len(servers_map))
		for i := 0; i< len(servers_map); i++ {
			value, exist := ports_map[i]
			if !exist {
				fmt.Println("Error when parsing settings.conf, missing server ports for lp"+strconv.Itoa(i+1))
				return nil, 0, nil, nil
			}
			local_ports[i] = value
		}
	}
	return
}
