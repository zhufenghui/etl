# -*- coding: utf-8 -*-
"""
Created on Tue Jul 07 17:39:43 2015

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

#initial for program_CN
class program_init:   #   program info 
     def __init__(self):
         self.programid = None
         self.type = None
         self.title = None
         self.genre = None
         self.sub_genre = None
         self.language = None
         self.rating = None
         self.cast_list = []
         self.crew_list = []
 
class cast_init:   
     def __init__(self):
         self.star_id = None
         self.star_name = None
         self.character_name = None
 
class crew_init:  
     def __init__(self):
         self.role = None
         self.star_id = None
         self.star_name = None   
         
class Program_xml:
    def GetProgramList(self):
        tree2 = ET.parse('c:/recsys/data/program_CN.xml')
        root2 = tree2.getroot()
        program_list=[]
        for pg in root2.iter('program'):
            program = program_init()
            program.programid = pg.get('id')
            program.type = pg.find('type').text
            program.title = pg.find('title').text
            program.genre = pg.find('genre').text
            program.sub_genre = pg.find('sub_genre').text
            program.language = pg.find('language').text
            program.rating = pg.find('rating').text
            program.cast_list = []
            program.crew_list = []
            for cst in pg.iter('cast'):
                cast = cast_init()
                cast.star_id = cst.find('star_id').text
                cast.star_name = cst.find('star_name').text
                cast.character_name = cst.find('character_name').text
                program.cast_list.append(cast)  
                
            for cr in pg.iter('crew'):
                crew = crew_init()
                crew.role = cr.get('role') 
                crew.star_id = cr.find('star_id').text
                crew.star_name = cr.find('star_name').text
                program.crew_list.append(crew)
            program_list.append(program)
        return program_list

def write2oracle(s,table,data):
#    s='scheduleid,epgcode, schedule_date, evnetid,eventop,program_id ,end_time, start_time, \
#    episode_number,related_teams,title' 
    tablename= table.strip("'")
    ls = s.split(',')
    colnames = s.strip("'")
    x = [':'+str(x+1) for x in range(len(ls))]
    nums=','.join(x)
    var = [ i+' '+'varchar2(200)' for i in ls]
    varr = ','.join(var).strip("'")
    create_query = "create table %s(%s)" % (tablename,varr)
    print create_query
    insert_query = "insert into  %s (%s) values (%s) " %(tablename, colnames, nums.strip("'"))
    print insert_query  
    dsn=co.makedsn('192.168.113.231','1521','orcl')
    conn=co.connect('ETL','ETL',dsn)
    cur=conn.cursor()
    try:
        cur.execute(create_query)
    except:
        print "table %s existed" % tablename
    cur.executemany(insert_query, data)
    cur.close()
    conn.commit()
    conn.close()
    return True
    
    
if __name__=='__main__':
       P = Program_xml()
       PL = P.GetProgramList()
       program_data=[] 
       program_casts = []
       program_crews = []
       for program in PL:
           t1 = (program.programid,program.type,program.genre,program.sub_genre,program.title,\
           program.language,program.rating)
           program_data.append(t1)
       for program in PL:
           if len(program.cast_list)==0:
               t2=(program.programid,program.title,None,None,None)
               program_casts.append(t2)
           else:
               for cast in program.cast_list:
                  t2 = (program.programid,program.title,cast.star_id,cast.star_name,cast.character_name)
                  program_casts.append(t2)
       for program in PL:
           if len(program.crew_list)==0:
               t3=(program.programid,program.title,None,None,None)
               program_casts.append(t2)
           else:
               for crew in program.crew_list:
                  t3 = (program.programid,program.title,crew.role,crew.star_id,crew.star_name)
                  program_crews.append(t3)
#       s= 'programid,type,genre,sub_genre,title,language,rating'
       s2 = 'programid,title,star_id,star_name,character_name'
       s3 = 'programid,title,role,star_id,star_name'
       write2oracle(s2,'program_casts',program_casts)
       write2oracle(s3,'program_crews',program_crews) 
       
           