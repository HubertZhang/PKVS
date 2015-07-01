__author__ = 'Hubert'
from client_ops import *

word_pool = ["key1", "_key2", "^%!@#$%^&*()key3", "{key4", "key5_+=",
             "key6-=_+-[", "key7测试", "{key8=", "_key9\'\"", "]key10\\|",
             "12421", "aslf\rjhalgha", "657468sv\0ca", "18726\a(^&(^(",
             "0chp3\"`", ")*HPB", "啦啦啦", "+++", "~!@#GX", "{\ndAFqw}"]

word_pool1 = ["df2738r7yweh","hjdf834","f3","j843","jsdf82342","jjjjf","jnd883","jhjf82","2548695","dfu3qw","jdf83"]

def normal_consistency_test(n): #random
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

def high_concurrency_test(n): # random
    m = len(word_pool)-1
    threads = []
    for i in range(0,n):
        act = randint(1,4)
        user = randint(1,3)
        print "------ %s: %s %s ------" %(str(i), str(user), str(act))

        if act == 1:
            t = th.Thread(target=get, args=(user, word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            threads.append(t)
            #t.join()
        elif act == 2:
            t = th.Thread(target=insert, args=(user, word_pool[randint(0,m)], word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            threads.append(t)
            #t.join()
        elif act == 3:
            t = th.Thread(target=update, args=(user, word_pool[randint(0,m)], word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            threads.append(t)
            #t.join()
        else:
            t = th.Thread(target=delete, args=(user, word_pool[randint(0,m)],))
            t.setDaemon(True)
            t.start()
            threads.append(t)
            #t.join()

    #time.sleep(10)
    i=0
    for t in threads:
        t.join()
        print i
        i+=1

def dump_test():
    d1 = organized_dump(1)
    d2 = organized_dump(2)
    d3 = organized_dump(3)
    result = False
    if d1 == d2 and d2 == d3:
        print "Perfect"
        print "d1",d1
        print "d2",d2
        print "d3",d3
        result = True
    else:
        print "Inconsistency!"
        print "d1",d1
        print "d2",d2
        print "d3",d3
        result = False
    print "Finish"
    return result

def main():
    read_config()
    start_all()
    normal_consistency_test(50)
    high_concurrency_test(200)
    result = dump_test()
    shutdown_all()
    if result:
        sys.exit(0)
    else:
        sys.exit(-1)

if __name__ == "__main__":
    main()