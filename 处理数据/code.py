#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import math
import csv

db = MySQLdb.connect("localhost","wyz","123","rec" )
cursor = db.cursor()





def Update(a,f):
	print len(a)
	for (i,j),k in a.items():
		sql1='select * from user where user_id="%s" and item_id="%s"' % (i,j)
		try:
			cursor.execute(sql1)
			result=cursor.fetchall()
			if(len(result)==0):
				sql2='insert into user values("%s","%s",0,0,0,0)' % (i,j)
				try:
					cursor.execute(sql2)
				except:
					print 'error1'
		except:
			print 'error2'
		sql3='update user set %s="%d" where user_id="%s" and item_id="%s"' % (f,k,i,j)
		try:
			cursor.execute(sql3)
		except:
			print 'error3'

a={}
b={}
c={}
d={}
f=open('tc.csv')
f_csv=csv.reader(f)
headers=next(f_csv)
length=len(headers)
for row in f_csv:
	# tmp=[]
	# for i in range(0,length):
	# 	tmp.append(row[i])
	# a.append(tmp)
	if(row[2]=='1'):
		a.setdefault((row[0],row[1]),0)
		a[(row[0],row[1])]=a[(row[0],row[1])]+1
	elif(row[2]=='2'):
		b.setdefault((row[0],row[1]),0)
		b[(row[0],row[1])]=b[(row[0],row[1])]+1
	elif(row[2]=='3'):
		c.setdefault((row[0],row[1]),0)
		c[(row[0],row[1])]=c[(row[0],row[1])]+1
	else:
		d.setdefault((row[0],row[1]),0)
		d[(row[0],row[1])]=d[(row[0],row[1])]+1


Update(a,"click_count")
Update(b,"love")
Update(c,"cart")
Update(d,"buy")










db.commit()
f.close()
db.close()