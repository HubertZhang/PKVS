# coding=utf-8
__author__ = "KevinXuxuxu"

import requests
import re
import os
import json
from  random import randint
import time
import subprocess
import threading as th
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

servers = [""]
servers_man = [""]

MANUALLY_DEBUG = True
SINGLE_PORT = True

def read_config(file = "./conf/settings.conf"):
    file = open(file, "r")
    config = json.load(file)
    if SINGLE_PORT:
        servers.append("http://%s:%s/kv/" %(re.split(':',config['n01'])[0],config['port']))
        servers.append("http://%s:%s/kv/" %(re.split(':',config['n02'])[0],config['port']))
        servers.append("http://%s:%s/kv/" %(re.split(':',config['n03'])[0],config['port']))
        servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n01'])[0],config['port']))
        servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n02'])[0],config['port']))
        servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n03'])[0],config['port']))
    else:
        servers.append("http://%s:%s/kv/" %(re.split(':',config['n01'])[0],config['lp01']))
        servers.append("http://%s:%s/kv/" %(re.split(':',config['n02'])[0],config['lp02']))
        servers.append("http://%s:%s/kv/" %(re.split(':',config['n03'])[0],config['lp03']))
        servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n01'])[0],config['lp01']))
        servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n02'])[0],config['lp02']))
        servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n03'])[0],config['lp03']))

def start(i):
    #subprocess.Popen([os.getcwd() + "/bin/server", str(i)], stdout=os.devnull, stderr=None)
    os.system("./bin/server %s > log%s &" %(str(i),str(i)))

def start_all():
    if MANUALLY_DEBUG :
        return
    start(1)
    start(2)
    start(3)

def get(me, key=""):
    if key == "":
        return
    params = {'key': key}
    r = requests.get(servers[me]+'get', params=params)
    return r.json(), r.elapsed.total_seconds()

def delete(me, key=""):
    if key == "":
        return
    payload = {'key': key}
    r = requests.post(servers[me]+'delete', data=payload)
    return r.json(), r.elapsed.total_seconds()

def insert(me, key="", value=""):
    if key == "":
        retrun
    payload = {'key': key, 'value': value}
    r = requests.post(servers[me]+'insert', data=payload)
    return r.json(), r.elapsed.total_seconds()

def update(me, key="", value=""):
    if key == "":
        retrun
    payload = {'key': key, 'value': value}
    r = requests.post(servers[me]+'update', data=payload)
    return r.json(), r.elapsed.total_seconds()

def shutdown(me):
    try:
        requests.get(servers_man[me]+'shutdown', params={})
    except Exception as e:
        print e

def shutdown_all():
    if MANUALLY_DEBUG:
        return
    shutdown(1)
    shutdown(2)
    shutdown(3)

def dump(me):
    r = requests.get(servers_man[me]+"dump", params={})
    return r.json(), r.elapsed.total_seconds()

def organized_dump(me):
    r, t = dump(me)
    d = {}
    for p in r:
        d[p[0]] = p[1]
    return d

def count(me):
    r = requests.get(servers_man[me]+'countkey', params={})
    return r.json(), r.elapsed.total_seconds()

