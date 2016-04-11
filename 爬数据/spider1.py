#encoding=utf-8

import math
import thread
import threading
import urllib2
import urllib
import MySQLdb
import sys
import os
import re
reload(sys)
sys.setdefaultencoding('utf8')


db = MySQLdb.connect("localhost","root","1115","wine",charset='utf8')

thread_num = 4
length = 0


url_prefix = "http://www.wine.com"
db_lock = threading.Lock()
wine_list = []
thread_list = []

re_web = re.compile(r'href="(.*?il.aspx)"')


re_productId = re.compile(r'"ProductId":\s*"(.*?)"')
re_pproductId = re.compile(r'"PproductId":\s*"(.*?)"')
re_title = re.compile(r'<title>(.*?)<',re.S)
re_region = re.compile(r'Region":\s*"(.*?)"',re.S)
re_varietalId = re.compile(r'"VarietalId":\s*"(.*?)"')
re_vineyardId = re.compile(r'"VineyardId":\s*"(.*?)"')
re_productType = re.compile(r'"ProductType":\s*"(.*?)"')
re_price = re.compile(r'"Price":\s*"(.*?)"')
re_picUrl = re.compile(r'"image"\s*src="(.*?)"',re.S)
re_user = re.compile(r'profile=(.*?)"')
re_ratingvalue = re.compile(r'ratingValue">(.*?)<')

def getData(offset):
    while True:
        if offset >= length:
            break
        url = wine_list[offset]
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

        productId = re.search(re_productId, html)
        if productId:
            productId = productId.group(1)
        else:
            productId = "null"


        pproductId = re.search(re_pproductId, html)
        if pproductId:
            pproductId = pproductId.group(1)
        else:
            pproductId = "null"

        title = re.search(re_title, html)
        if title:
            title = title.group(1)
        else:
            title = "null"

        region = re.search(re_region, html)
        if region:
            region = region.group(1)
        else:
            region = "null"

        varietalId = re.search(re_varietalId, html)
        if varietalId:
            varietalId = varietalId.group(1)
        else:
            varietalId = "null"

        vineyardId = re.search(re_vineyardId, html)
        if vineyardId:
            vineyardId = vineyardId.group(1)
        else:
            vineyardId = "null"

        productType = re.search(re_productType, html)
        if productType:
            productType = productType.group(1)
        else:
            productType = "null"

        price = re.search(re_price, html)
        if price:
            price = price.group(1)
        else:
            price = "null"

        picUrl = re.search(re_picUrl, html)
        if picUrl:
            picUrl = picUrl.group(1)
        else:
            picUrl = "null"

        if picUrl[0] == '/' and picUrl[1] == '/':
            picUrl = "http:"+picUrl


        user = []
        username = re.findall(re_user,html)
        for uu in username:
            user.append(uu)
        rate = []
        rating = re.findall(re_ratingvalue,html)
        lenOfRating = len(rating)
        if lenOfRating > 2:
            for index in range(2,lenOfRating):
                rate.append(rating[index])

        path = os.getcwd()
        pic_path = "/home/wyz/ecust/wine/PIC/%s.jpg" % (productId)
        urllib.urlretrieve(picUrl, pic_path)

        print offset

        print url,productId

        if db_lock.acquire():
            sql = 'insert into winedata values("%s","%s","%s","%s","%s","%s","%s","%s")' % (productId,pproductId,title,region,varietalId,vineyardId,productType,price)
            cursor = db.cursor()
            cursor.execute(sql)
            llen = len(user)
            if llen==len(rate):
                for i in range(0,llen):
                    sql = 'select * from winereview where productId="%s" and username="%s"' % (productId, user[i])
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    if len(results)>0:
                        continue
                    sql = 'insert into winereview values("%s","%s","%s")' % (productId,user[i],rate[i])
                    cursor.execute(sql)
            db.commit()
            db_lock.release()


        offset = offset + thread_num

    pass


def main():
    cursor = db.cursor()
    sql = "select * from wineweb"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        wine_list.append(row[0])
    global length
    length = len(wine_list)

    for i in range(0,thread_num):
        thread_list.append(threading.Thread(target=getData,args=(i,)))
    for td in thread_list:
        td.start()



    for td in thread_list:
        td.join()


    db.commit()
    db.close()
    pass

def test():
    cursor = db.cursor()
    sql = "select * from wineweb limit 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        wine_list.append(row[0])
    global length
    length = len(wine_list)

    print wine_list[0]

    getData(0)

    db.commit()
    db.close()
    pass

if __name__ == '__main__':
    main()