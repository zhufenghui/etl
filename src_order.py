# -*- coding: utf-8 -*-
"""
Created on Wed Jul 01 10:10:22 2015

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
##从目录下读取文件，拼接

def getDataFrame(path):
#    path='c:/recsys/data/zs_card.txt'
    l=[]
    ind=[]
    fixed_cols = ['client_type','devno','region_code','network_id','create_time','service_type']
    with open(path) as f :
        for line in f:
            line=line.strip()
            newline=re.split('\||\^',line) 
    #        newline[4]=pd.to_datetime(newline[4])
            l.append(newline)
    df = pd.DataFrame(l)
    param_len = len(df.columns)-len(fixed_cols)
    cols=fixed_cols+['param'+str(x) for x in range(param_len)] 
    df.columns=cols
    print df.columns
    df = df.sort(['devno','create_time'],ascending=[True,True])
    # add the endtime to df 
    et=df.groupby('devno')['create_time'].shift(-1)
    df['endtime']=et
    df = df.where((pd.notnull(df)), 'null')
    # 将所有columns转换为str才能插入 oracle， notype和str在一个字段中混合导致executemany出错
    #for i in range(len(cl)):
    #    df[cl[i]]=df[cl[i]].astype('str')
    return df 

def getMaxLen(df):
    cols = df.columns
    ls=[]
    for i in cols:
        l = df[i].map(lambda x: len(x)).max()
        ls.append(l)
    ld = dict(zip(cols,ls))
    return ld
    
def getCreateSql(tablename,df):
    tablename = tablename.strip("'") 
    cols = df.columns
    m= getMaxLen(df)
    var = [ i+' '+'varchar2(%s)'%(m[i]+10)  for i in cols]
    varr = ','.join(var).strip("'")
    create_query = "create table %s(%s)" % (tablename,varr)
    return create_query
    
def getInsertSql(tablename,df):
    tablename= tablename.strip("'") 
    cols = df.columns
    insert_cols = ','.join(cols).strip("'")
    x = [':'+str(x+1) for x in range(len(cols))]
    nums=','.join(x).strip("'")
    insert_query = "insert into  %s (%s) values (%s) " %(tablename, insert_cols, nums)
    return insert_query
    
def write2oracle(tablename,df):
    data=list(np.asanyarray(df))
    dsn=co.makedsn('192.168.113.231','1521','orcl')
    conn=co.connect('ETL','ETL',dsn)
    cur=conn.cursor()
    create_query = getCreateSql(tablename,df)
    insert_query = getInsertSql(tablename,df)
    try:
        cur.execute(create_query)
    except:
        print "table %s existed" % tablename
    cur.executemany(insert_query, data)
    cur.close()
    conn.commit()
    conn.close()
    return True
    
def write2mysql():
    conn = md.connect(host='localhost',port = 3306, user='root', passwd='zsboss', db ='test',charset='utf8')  #charset 
    df.to_sql('raw_data',conn,flavor='mysql',if_exists='repalce')
    return True

 
if __name__=='__main__':
    path='c:/recsys/data/rf1.txt'
    df = getDataFrame(path)
    write2oracle('src_order',df)


