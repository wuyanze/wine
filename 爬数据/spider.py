#encoding=utf-8

import math
import thread
import threading
import urllib2
import urllib
import MySQLdb
import sys
import re
reload(sys)
sys.setdefaultencoding('utf8')


db = MySQLdb.connect("localhost","root","1115","wine",charset='utf8')

thread_num=8
page_num=59


set_lock = threading.Lock()
wine_set = set()
thread_list1 = []
prefix="http://cn.pudaowines.com"

re_web = re.compile(r"href='(.*?html)' >")


def get_wine_set(offset):
    while(True):
        if(offset>page_num):
            break
        url="http://cn.pudaowines.com/chi/buy-wines-spirits/Page-%d/?types[]=0" % (offset)
        page = urllib.urlopen(url)
        html = page.read()
        match=re.findall(re_web,html)
        if set_lock.acquire():
            for i in match:
                wine_set.add(i)
            set_lock.release()
        offset=offset+8


def main():
    for i in range(1,thread_num+1):
        thread_list1.append(threading.Thread(target=get_wine_set,args=(i,)))
    for i in thread_list1:
        i.start()
    for i in thread_list1:
        i.join()
    cursor = db.cursor()
    for SET in wine_set:
        sql = 'insert into website values("%s")' % (prefix+SET)
        cursor.execute(sql)
    db.commit()
    db.close()
    pass

if __name__ == '__main__':
    main()