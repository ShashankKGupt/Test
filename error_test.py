import pymongo
import sqlite3
import pandas as pd
import numpy as np
import os
import shashank
os.chdir(r'C:\Users\esaugph\Desktop')
conn=pymongo.MongoClient('localhost',27017)
mydb=conn['employee']
conn1 = sqlite3.connect('flights.db')
cursor = conn1.cursor()
R_list=[]
Al_list=[]
AP_list=[]
RT=cursor.execute("select * from Routes limit 1000")
for i in RT:
    R_list.append(i)
AL=cursor.execute("select * from Airlines where id in (select airline_id from routes limit 1000);")
for i in AL:
    Al_list.append(i)
AP1=cursor.execute("select * from Airports where id in (select source_id from routes limit 1000);")
AP2=cursor.execute("select * from Airports where id in (select dest_id from routes limit 1000);")
for i in AP1:
    AP_list.append(i)
for i in AP2:
    AP_list.append(i)
dict1={}
list1=[]
APC=['index', 'id', 'name', 'city', 'country', 'code', 'icao', 'latitude', 'longitude', 'altitude', 'offset', 'dst', 'timezone']
ALC=['index', 'id', 'name', 'alias', 'iata', 'icao', 'callsign', 'country', 'active']
RTC=['index', 'airline', 'airline_id', 'source', 'source_id', 'dest', 'dest_id', 'codeshare', 'stops', 'equipment']
for i in R_list:
	for j in Al_list:
		if(i[2]==j[1]):
			a={ALC[0]:j[0], ALC[1]:j[1], ALC[2]:j[2], ALC[3]:j[3], ALC[4]:j[4], ALC[5]:j[5], ALC[6]:j[6], ALC[7]:j[7], ALC[8]:j[8]}
	for k in AP_list:
		if(i[4]==k[1]):
			b={APC[0]:k[0], APC[1]:k[1], APC[2]:k[2], APC[3]:k[3], APC[4]:k[4], APC[5]:k[5], APC[6]:k[6], APC[7]:k[7], APC[8]:k[8], APC[9]:k[9], APC[10]:k[10], APC[11]:k[11], APC[12]:k[12]}
	for k in AP_list:
		if(i[6]==k[1]):
			c={APC[0]:k[0], APC[1]:k[1], APC[2]:k[2], APC[3]:k[3], APC[4]:k[4], APC[5]:k[5], APC[6]:k[6], APC[7]:k[7], APC[8]:k[8], APC[9]:k[9], APC[10]:k[10], APC[11]:k[11], APC[12]:k[12]}
	dict1={RTC[0]:i[0], RTC[1]:i[1], RTC[2]:a, RTC[3]:i[3], RTC[4]:b, RTC[5]:i[5], RTC[6]:c, RTC[7]:i[7], RTC[8]:i[8], RTC[9]:i[9]}
	list1.append(dict1)
mydb.AirPort.insert_many(list1)



