#encoding=utf-8

import math
import thread
import threading
import urllib2
import urllib
import MySQLdb
import sys
import string
import re
import os
reload(sys)
sys.setdefaultencoding('utf8')


db = MySQLdb.connect("localhost","root","1115","wine",charset='utf8')
cursor = db.cursor()

thread_num = 6
web_list = []
length = 0
thread_list = []


mysql_lock = threading.Lock()

#re
re_title = re.compile(r'<h1><b>(.*?)<')
re_subtitle = re.compile(r'biao">(.*?)<')
re_price = re.compile(r'￥(.*?)<')
re_type = re.compile(r'>种类：<\/span>(.*?)<')
re_country = re.compile(r'国家：<\/span>(.*?)<')
re_vol = re.compile(r'n>(.*?)vol')
re_ocaasion = re.compile(r'场合：<\/span>(.*?)<')
re_grape = re.compile(r'葡萄种类：<\/span>(.*)<')
re_pic = re.compile(r"<a href='(.*?)' c")




path_prefix = "/home/wyz/ecust/wine/pic"

def func(offset):
    while(True):
        if offset>=length :
            break
        url = web_list[offset]
        id = url.split("/")[4].split(".")[0]
        id = string.atoi(id,10)
        fails = 0
        while True:
            if fails>3:
                break
            try:
                page = urllib2.urlopen(url,timeout=10)
                html = page.read()
                break
            except:
                fails = fails+1

        title = re.search(re_title,html)
        if title:
            title = title.group(1)
        else:
            title = "null"

        subtitle = re.search(re_subtitle, html)
        if subtitle:
            subtitle = subtitle.group(1)
        else:
            subtitle = "null"

        price = re.search(re_price, html)
        if price:
            price = price.group(1)
        else:
            price = "null"

        type = re.search(re_type, html)
        if type:
            type = type.group(1)
        else:
            type = "null"

        country = re.search(re_country, html)
        if country:
            country = country.group(1)
        else:
            country = "null"

        vol = re.search(re_vol, html)
        if vol:
            vol = vol.group(1)
        else:
            vol = "null"

        grape = re.search(re_grape, html)
        if grape:
            grape = grape.group(1)
        else:
            grape = "null"

        pic = re.search(re_pic, html)
        if pic:
            pic = pic.group(1)
        else:
            pic = "http://img0.imgtn.bdimg.com/it/u=215837620,3198435686&fm=116&gp=0.jpg"

        occasion = re.search(re_ocaasion, html)
        if occasion:
            occasion = occasion.group(1)
        else:
            occasion = "null"

        path = "/%d.jpg" % (id)
        path = path_prefix + path

        if os.path.isfile(path)==False:
            urllib.urlretrieve(pic, path)
        print offset

        if mysql_lock.acquire():
            sql='insert into data values(%d,"%s","%s","%s","%s","%s","%s","%s","%s")' % (id,title,subtitle,price,type,country,vol,occasion,grape)
            cursor1=db.cursor()
            cursor1.execute(sql)
            db.commit()
            mysql_lock.release()


        offset=offset+thread_num
    pass


def main():
    sql = "select * from web"
    cursor.execute(sql)
    cursor.fetchall()
    for url in cursor:
        web_list.append(url[0])
    global length
    length = len(web_list)
    for i in range(thread_num):
        thread_list.append(threading.Thread(target=func,args=(i,)))
    for td in thread_list:
        td.start()
    for td in thread_list:
        td.join()
    db.close()
    pass

if __name__ == '__main__':
    main()