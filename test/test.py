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

def read_config(file = "./conf/settings.conf"):
    file = open(file, "r")
    config = json.load(file)
    servers.append("http://%s:%s/kv/" %(re.split(':',config['n01'])[0],config['port01']))
    servers.append("http://%s:%s/kv/" %(re.split(':',config['n02'])[0],config['port02']))
    servers.append("http://%s:%s/kv/" %(re.split(':',config['n03'])[0],config['port03']))
    servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n01'])[0],config['port01']))
    servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n02'])[0],config['port02']))
    servers_man.append("http://%s:%s/kvman/" %(re.split(':',config['n03'])[0],config['port03']))

def start(i):
    #subprocess.Popen([os.getcwd() + "/bin/server", str(i)], stdout=os.devnull, stderr=None)
    os.system("./bin/server %s > ~/Desktop/log%s &" %(str(i),str(i)))

def start_all():
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

temp = ["key1", "_key2", "^%!@#$%^&*()key3", "{key4", "key5_+=",
             "key6-=_+-[", "key7测试", "{key8=", "_key9\'\"", "]key10\\|",
             "12421", "aslf\rjhalgha", "657468sv\0ca", "18726\a(^&(^(",
             "0chp3\"`", ")*HPB", "啦啦啦", "+++", "~!@#GX", "{\ndAFqw}"]

word_pool = ["df2738r7yweh","hjdf834","f3","j843","jsdf82342","jjjjf","jnd883","jhjf82","2548695","dfu3qw","jdf83"]

def normal_consistency_test(n): #random
    start_all()
    m = len(word_pool)-1
    d = {}
    for i in range(0,n):
        act = randint(1,4)
        print "------ %s: %s ------" %(str(i),str(act))
        if act == 1:
            if len(d) > 0 and randint(1,5) > 1:
                key = d.keys()[randint(0,len(d)-1)]
            else:
                key = word_pool[randint(0,m)]
            r, t = get(randint(1,3), key=key)
            if (not r['success'] and d.has_key(key)) or (not d.has_key(key) and r['success']):
                print "error on operation #%s: GET key=%s" %(str(i), key)
                break
            elif r['success'] and r['value'] != d[key]:
                print "error on operation #%s: GET key=%s conflict with value1=%s, value2=%s" %(str(i), key, r['value'],d[key])
                break
        elif act == 2:
            key = word_pool[randint(0,m)]
            value = word_pool[randint(0,m)]
            r, t = insert(randint(1,3), key=key, value=value)
            if (not r['success'] and not d.has_key(key)) or (r['success'] and d.has_key(key)):
                print "error on operation #%s: INSERT conflict, key=%s" %s(str(i), key)
                break
            if r['success']:
                d[key] = value
        elif act == 3:
            key = word_pool[randint(0,m)]
            value = word_pool[randint(0,m)]
            r, t = update(randint(1,3), key=key, value=value)
            if (not r['success'] and d.has_key(key)) or (not d.has_key(key) and r['success']):
                print "error on operation #%s: UPDATE conflict, key=%s" %s(str(i), key)
                break
            if r['success']:
                d[key] = value
        else:
            key = word_pool[randint(0,m)]
            r, t = delete(randint(1,3), key=key)
            if (not r['success'] and d.has_key(key)) or (not d.has_key(key) and r['success']):
                print "error on operation #%s: DELETE conflict, key=%s" %s(str(i), key)
                break
            if r['success']:
                d.pop(key)
    d1 = organized_dump(1)
    d2 = organized_dump(2)
    d3 = organized_dump(3)
    if d1 == d2 and d2 == d3:
        print "Perfect"
    else:
        print "Inconsistency!"
        print "d",d
        print "d1",d1
        print "d2",d2
        print "d3",d3
    print "Finish"
    shutdown_all()

def high_concurrency_test(n): # random
    start_all()
    time.sleep(2)
    m = len(word_pool)-1
    for i in range(0,n):
        act = randint(1,4)
        print "------ %s: %s ------" %(str(i), str(act))
        if act == 1:
            t = th.Thread(target=get, args=(randint(1,3), word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            #t.join()
        elif act == 2:
            t = th.Thread(target=insert, args=(randint(1,3), word_pool[randint(0,m)], word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            #t.join()
        elif act == 3:
            t = th.Thread(target=update, args=(randint(1,3), word_pool[randint(0,m)], word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            #t.join()
        else:
            t = th.Thread(target=delete, args=(randint(1,3), word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            #t.join()
    time.sleep(10)
    d1 = organized_dump(1)
    d2 = organized_dump(2)
    d3 = organized_dump(3)
    if d1 == d2 and d2 == d3:
        print "Perfect"
    else:
        print "Inconsistency!"
        print "d1",d1
        print "d2",d2
        print "d3",d3

    print "Finish"
    shutdown_all()


def main():
    read_config()
    #normal_consistency_test(50)
    high_concurrency_test(20)

if __name__ == "__main__":
    main()
