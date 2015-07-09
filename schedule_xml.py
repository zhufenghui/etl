# -*- coding: utf-8 -*-
"""
Created on Mon May 04 16:15:00 2015

@author: admin
"""
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  #必须置于cx之前, 解决Oracle导入中文乱码问题
import cx_Oracle as co
import pandas as pd
import numpy as np 
import re
import time
import MySQLdb as md
import xml.etree.ElementTree as ET

#initial for shcedule_CN 
class EventElement:
      def __init__(self): 
           self.eventid = None
           self.program_id = None
           self.start_time= None
           self.end_time = None
           self.title = None
           self.episode_number = None
           self.related_teams = None      
           
class ScheduleElement:
      def __init__(self):           
           self.scheduleid = None
           self.event_list = [] #contains a list of  
               
class Schedule_xml:   
       def GetScheduleList(self):
           tree = ET.parse('c:/recsys/data/schedule_CN.xml')
           root = tree.getroot()
           schedule_list =[]
           for scd  in root.iter('schedule'):
               schedule = ScheduleElement()
               schedule.scheduleid = scd.get('channel_id')
               schedule.epgcode = scd.get('epg_code')
               schedule.date = scd.get('date')
               schedule.event_list = []
               for evt in  scd.iter('event'):
                   event = EventElement()
                   event.id = evt.get('id') 
                   event.op = evt.get('op')
                   event.program_id = evt.find('program_id').text
                   event.end_time=evt.find('end_time').text
                   event.episode_number = evt.find('episode_number').text
                   event.related_teams = evt.find('related_teams').text
                   event.start_time = evt.find('start_time').text
                   event.title = evt.find('title').text
                   schedule.event_list.append(event)
               schedule_list.append(schedule)
           return schedule_list

def write2oracle(data):
    s='scheduleid,epgcode, schedule_date, evnetid,eventop,program_id ,end_time, start_time, \
    episode_number,related_teams,title' 
    tablename='schedule'.strip("'")
    ls = s.split(',')
    colnames = s.strip("'")
    x = [':'+str(x+1) for x in range(len(ls))]
    nums=','.join(x)
    var = [ i+' ''varchar2(40)' for i in ls]
    varr = ','.join(var).strip("'")
    create_query = "create table %s(%s)" % (tablename,varr)
    print create_query
    insert_query = "insert into  %s (%s) values (%s) " %(tablename, colnames, nums.strip("'"))
    print insert_query  
    dsn=co.makedsn('192.168.113.231','1521','orcl')
    conn=co.connect('ETL','ETL',dsn)
    cur=conn.cursor()
#    cur.execute(create_query)
    cur.executemany(insert_query, data)
    cur.close()
    conn.commit()
    conn.close()
    return True
    

if __name__=='__main__':
       S = Schedule_xml()
       SL=S.GetScheduleList()
       schedule_data=[]
       for schedule in SL:
           for event in  schedule.event_list:
#                print event.related_teams
               schedule_line= ( schedule.scheduleid,schedule.epgcode,schedule.date, \
               event.id,event.op,event.program_id  ,event.end_time, event.start_time,  \
               event.episode_number,event.related_teams,event.title )
               schedule_data.append(schedule_line)
       
       write2oracle(schedule_data) 
           
 
               
