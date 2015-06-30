# use UTF-8
__author__ = "KevinXuxuxu"

import requests
import re
import os
import json
import random
import time
import subprocess
import threading

servers = [""]
servers_man = [""]

def read_config(file = "./conf/settings.conf"):
    file = open(file, "r")
    config = json.load(file)
    servers.append("%s:%s/kv/" %(re.split(':',config['n01'])[0],config['port01']))
    servers.append("%s:%s/kv/" %(re.split(':',config['n02'])[0],config['port02']))
    servers.append("%s:%s/kv/" %(re.split(':',config['n03'])[0],config['port03']))
    servers_man.append("%s:%s/kvman/" %(re.split(':',config['n01'])[0],config['port01']))
    servers_man.append("%s:%s/kvman/" %(re.split(':',config['n02'])[0],config['port02']))
    servers_man.append("%s:%s/kvman/" %(re.split(':',config['n03'])[0],config['port03']))

def start(i):
    subprocess.Popen([os.getcwd() + "/bin/server", str(i)], stdout=os.devnull, stderr=None)

def get(me, key=""):
    if key == "":
        return
    params = {'key': key}
    r = requests.get(servers[me]+'get', params=params, header={"Content-Type": "application/x-www-form-urlencoded"})
    return r.json(), r.elapsed.total_seconds()

def delete(me, key=""):
    if key == "":
        return
    payload = {'key': key}
    r = requests.post(servers[me]+'delete', data=payload, header={"Content-Type": "application/x-www-form-urlencoded"})
    return r.json(), r.elapsed.total_seconds()

def insert(me, key="", value=""):
    if key == "":
        retrun
    payload = {'key': key, 'value': value}
    r = requests.post(servers[me]+'insert', data=payload, header={"Content-Type": "application/x-www-form-urlencoded"})
    return r.json(), r.elapsed.total_seconds()

def update(me, key="", value=""):
    if key == "":
        retrun
    payload = {'key': key, 'value': value}
    r = requests.post(servers[me]+'update', data=payload, header={"Content-Type": "application/x-www-form-urlencoded"})
    return r.json(), r.elapsed.total_seconds()

def random_test():
    start(1)
    start(2)
    start(3)
