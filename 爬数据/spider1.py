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
cursor = db.cursor()

thread_num = 8
web_list = []
length = 0
thread_list = []


mysql_lock = threading.Lock()

#re
re_name = re.compile(r'<title>(.*?)</ti')
re_country = re.compile(r'国家</div>.*?>(.*?)<',re.S)
re_type = re.compile(r'类型</div>.*?>(.*?)<',re.S)
re_maker = re.compile(r'产商</div>.*?>(.*?)<',re.S)
re_region = re.compile(r'地区.*?<!.*?>(.*?)<',re.S)
re_color = re.compile(r'颜色</div>.*?>(.*?)<',re.S)
re_capacity = re.compile(r'容量</div>.*?>(.*?)<',re.S)
re_grape = re.compile(r'品种.*?<!.*?>(.*?)<',re.S)
re_pic = re.compile(r"'(.*?.jpg)' c")

prefix = "http://cn.pudaowines.com"
path_prefix = "/home/wyz/ecust/wine/pic"

def func(offset):
    while(True):
        if offset>=length :
            break
        url = web_list[offset]
        url = urllib.quote(url)
        url = "http:"+url[7:]
        page = urllib.urlopen(url)
        html = page.read()
        name = re.search(re_name,html).group(1)
        country = re.search(re_country,html).group(1)
        type = re.search(re_type,html).group(1)
        maker = re.search(re_maker,html).group(1)
        region = re.search(re_region,html).group(1)
        color = re.search(re_color,html).group(1)
        capacity = re.search(re_capacity, html).group(1)
        grape = re.search(re_grape, html).group(1)
        pic = re.search(re_pic, html).group(1)
        pic = prefix+pic
        path = "/%d.jpg" % (offset)
        path = path_prefix+path
        urllib.urlretrieve(pic,path)
        if mysql_lock.acquire():
            sql='insert into init values(%d,"%s","%s","%s","%s","%s","%s","%s","%s")' % (offset,name,country,type,maker,region,color,capacity,grape)
            cursor.execute(sql)
            mysql_lock.release()


        offset=offset+8
    pass


def main():
    sql = "select * from website"
    cursor.execute(sql)
    cursor.fetchall()
    for url in cursor:
        web_list.append(url[0])
    global length
    length = len(web_list)
    print length
    for i in range(thread_num):
        thread_list.append(threading.Thread(target=func,args=(i,)))
    for td in thread_list:
        td.start()
    for td in thread_list:
        td.join()
    db.commit()
    db.close()
    pass

if __name__ == '__main__':
    main()