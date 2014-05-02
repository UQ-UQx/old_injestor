#!/usr/bin/python
import hashlib

import os
import time
import baseservice
import peewee
from peewee import *
import MySQLdb

basepath = os.path.dirname(__file__)

class SQLImport(baseservice.BaseService):

    inst = None

    def __init__(self):
        SQLImport.inst = self
        super(SQLImport, self).__init__()

        self.status['name'] = "SQL Importer"
        self.sql_enabled = True
        self.sql_dbname = 'uqxdump'
        self.sql_tablename = ""
        self.initialize()

    def setup(self):
        pass

    def run(self):
        self.setaction('test running')
        self.status['status'] = 'running'
        #load a file
        while self.load_incoming_file():
            #edge to ignore
            if self.filename.find("prod-edge") > -1:
                self.movetofinish()
                continue
            columns = []
            #split the headers
            for line in self.file:
                columns = line.split("\t")
                break
            self.setaction("creating table for "+self.filename)
            if self.createtableandvalid(self.filename,columns):
                self.setaction("loading data from "+self.filename)
                self.status['progress']['total'] = self.numlines()
                self.status['progress']['current'] = 0
                self.parselines(self.parseline)
            self.movetofinish()

    def createtableandvalid(self,tablename,columns=[]):
        isvalid = False
        print tablename
        if self.filename.find('UQx-') > -1 and self.filename.find('-prod-analytics.sql') > -1:
            isvalid = True
        if not isvalid:
            return isvalid
        # Remove date
        tablename = tablename[tablename.find('UQx-'):]
        # Remove Prod analytics
        tablename = tablename[:tablename.find('-prod-analytics.sql')]
        # Find the last dash, assume its the database, figure out the tablename
        usedb = tablename.split("-")
        tablename = usedb[len(usedb)-1]
        usedb = '_'.join(usedb)
        usedb = usedb.replace("_"+tablename,"")
        # Change to the database
        self.connect_to_sql(usedb)
        query = ""
        query += "CREATE TABLE IF NOT EXISTS "
        query += tablename
        query += " ( "
        for column in columns:
            coltype = "varchar(255)"
            if column == "key":
                column = "_key"
            if column == "state" or column == "content" or column == "meta":
                coltype = "longtext"
            if column == "goals" or column == "mailing_address":
                coltype = "text"
            query += column.replace("\n","")+" "+coltype+", "
        query += " xhash varchar(200) "
        query += ", UNIQUE (xhash)"
        query += " );"
        self.sql_query(query)
        self.sql_tablename = tablename
        return isvalid

    def parseline(self,line):
        if line[:2] == 'id':
            return
        datahash = hashlib.sha256(line).hexdigest()
        line = line.replace("\n","")
        line = line.replace('"',"''")
        data = line.split("\t")
        data.append(datahash)
        insertdata = '"'+'","'.join(data)+'"'
        self.sql_query("INSERT IGNORE INTO "+self.sql_tablename+" VALUES ( "+insertdata+" );",True)
        self.status['progress']['current'] += 1

def name():
    return str("sqlimport")

def status():
    return SQLImport.inst.status

def runservice():
    return SQLImport()