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

thread_num=4
page_num=11200


url_prefix = "http://www.wine.com"
set_lock = threading.Lock()
wine_set = set()
thread_list1 = []

re_web = re.compile(r'href="(.*?il.aspx)"')




def get_wine_set(offset):
    while True:
        if offset > page_num:
            break
        url="http://www.wine.com/v6/wineshop/default.aspx?pagelength=100&Nao=%d" % (offset)
        fails = 0
        while True:
            if fails > 3:
                break
            try:
                page = urllib2.urlopen(url, timeout=10)
                html = page.read()
                break
            except:
                fails = fails + 1

        match=re.findall(re_web,html)
        print offset/100
        if set_lock.acquire():
            for i in match:
                wine_set.add(url_prefix+i)
            set_lock.release()
        offset = offset + 400


def main():
    for i in range(0,thread_num):
        thread_list1.append(threading.Thread(target=get_wine_set,args=(100*i,)))
    for i in thread_list1:
        i.start()
    for i in thread_list1:
        i.join()
    cursor = db.cursor()
    for SET in wine_set:
        sql = 'insert into wineweb values("%s")' % (SET)
        cursor.execute(sql)
    db.commit()
    db.close()
    pass

if __name__ == '__main__':
    main()