# -*- coding: utf-8 -*-
"""
Created on Thu Jul 09 08:49:27 2015

@author: admin
"""

from ftplib import FTP
localdir='c:/recsys/data/tvm/'
ftp = FTP()
timeout = 30
port = 20125
ftp.connect('124.160.149.201',port,timeout) # 连接FTP服务器
ftp.login('guangdongguangdian','61sHkADz') # 登录
print ftp.getwelcome()  # 获得欢迎信息 
#ftp.cwd('file/test')    # 设置FTP路径
list = ftp.nlst()       # 获得目录列表
for name in list:
    localpath =  localdir + name      # 文件保存路径
    print localpath
    f = open(localpath,'wb')         # 打开要保存文件
    filename = 'RETR ' + name   # 保存FTP文件
    ftp.retrbinary(filename,f.write) # 保存FTP上的文件 
ftp.quit()                  # 退出FTP服务器

 
