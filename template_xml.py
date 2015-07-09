# -*- coding: utf-8 -*-
"""
Created on Mon May 04 14:29:20 2015

@author: admin
"""
import re 
import xml.etree.ElementTree as ET
doc="""1 <?xml version="1.0"?>
 2 <mysqlconfig>
 3  <database>
 4      <host>127.0.0.1</host>
 5      <username>root</username>
 6      <password>123456</password>
 7      <port>3306</port>
 8      <instance name="test"> 
 9          <export-import tablename="test"> 
10              <exportConfigure>export 1.1.1</exportConfigure>
11              <exportDelimiter>|</exportDelimiter>
12              <importConfigure>import 1.1</importConfigure>
13          </export-import>
14          <export-import tablename="test">
15              <exportConfigure>export 1.1.2</exportConfigure>
16              <exportDelimiter>|</exportDelimiter>
17              <importConfigure>import 1.1.2</importConfigure>
18          </export-import>
19      </instance>
20      <instance name="test1">
21          <export-import tablename="users">
22              <exportConfigure>export 1.2.1</exportConfigure>
23              <exportDelimiter>|</exportDelimiter>
24              <importConfigure>import 1.2.1</importConfigure>
25          </export-import>
26          <export-import tablename="sss">
27              <exportConfigure>export 1.2.2</exportConfigure>
28              <importConfigure>import 1.2.2</importConfigure>
29          </export-import>
30      </instance>
31  </database>
32  <database>
33  <host>127.0.0.1</host>
34  <username>test</username>
35  <password>test</password>
36  <port>3306</port>
37  <instance name="test_it">
38      <export-import tablename="user_info">
39          <exportConfigure>select * from test </exportConfigure>
40          <exportDelimiter>|</exportDelimiter>
41          <exportFilePrefix>./data/</exportFilePrefix>
42          <importConfigure>import 2.2.1</importConfigure>
43      </export-import>
44  </instance>
45  </database>
46 </mysqlconfig>
"""
   
class ExportImportConfig:
       def __init__(self):
           self.tablename = None
           self.exportConfig = None
           self.exportDelimiter= None
           self.exportFilePrefix = None
           self.importExport = None
class InstanceConfig:
       def __init__(self):
           self.instanceName = None
           self.ExportImportConfigList = [] #contains a list of InstanceConfig
   
class DatabaseConfig:
       def __init__(self):
           self.host = None
           self.user = None
           self.password = None
           self.port = None
           self.instanceConfigList = []     #contains a list of InstanceConfig
   
   
def getMysqlImportExportConfig():
       tree = ET.parse('mysql_export_source_config.xml')
       root = tree.getroot()
       databaseList =[]
       for db  in root.iter('database'):
           database  = DatabaseConfig()
           database.host = db.find('host').text
           database.user = db.find('username').text 
           database.password = db.find('password').text
           database.port = int(db.find('port').text)
           database.instanceConfigList = []
           for inst in db.iter('instance'):
               instance = InstanceConfig()
               instance.instanceName = inst.get('name') 
               for ex_im in db.iter('export-import'):            
                   ex_im_config = ExportImportConfig()
                   ex_im_config.tablename = ex_im.get('tablename')
                   ex_im_config.exportConfig = ex_im.find('exportConfigure').text
                   ex_im_config.exportDelimiter = ex_im.find('exportDelimiter').text
                   ex_im_config.importExport = ex_im.find('importConfigure').text
                   ex_im_config.exportFilePrefix = ex_im.find('exportFilePrefix').text
   #                print ex_im_config.tablename,ex_im_config.exportConfig ,ex_im_config.importExport
                   print ex_im_config.exportDelimiter,ex_im_config.exportFilePrefix
                   instance.ExportImportConfigList.append(ex_im_config)
               database.instanceConfigList.append(instance)
           databaseList.append(database)
       return databaseList
   
if __name__=='__main__':
       exportlist = getMysqlImportExportConfig()
       print 'lend exportlist =' ,len(exportlist)
       for db in exportlist:
           print db.user,db.password,db.port
           for inst in db.instanceConfigList:
               print db.instanceConfigList  


 