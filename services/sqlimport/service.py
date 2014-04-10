#!/usr/bin/python
import hashlib

import os
import time
import baseservice
import peewee
from peewee import *
import MySQLdb

basepath = os.path.dirname(__file__)

sqldb = None

class SQLImport(baseservice.BaseService):

    usemongo = False
    sqluser = 'root'
    sqlpass = 'r3m0teAXS'
    sqldbname = 'mysql'
    sqldb = None
    sqltablename = ""

    status = {
        'name':'unknown',
        'status':'stopped',
        'action':'stopped',
        'actiontime':'000-00-00 00:00:00',
        'progress':{
            'current':'0',
            'total':'0'
        },
        'lastawake':'0000-00-00 00:00:00'
    }

    def __init__(self):
        self.status['name'] = "SQL Importer"
        self.initialize()

    def filelength(self):
        for i, l in enumerate(self.file):
            pass
        self.file.seek(0)
        return i + 1

    def setaction(self,theaction):
        if(theaction == 'stopped'):
            self.status['status'] = 'stopped'
        else:
            self.status['status'] = 'running'
        self.status['action'] = str(theaction)
        self.status['actiontime'] = time.strftime('%Y-%m-%d %H:%M:%S')

    def setup(self):
        self.changedb(self.sqldbname,True)
        pass

    def changedb(self,dbname,force=False):
        if self.sqldbname != dbname or force:
            try:
                newdb = MySQLdb.connect(host="localhost",user=self.sqluser,passwd=self.sqlpass,db=dbname)
                self.sqldb = newdb
                self.sqldbname = dbname
            except Exception, e:
                if e[0]:
                    print "Creating database"
                    self.doquery("CREATE DATABASE "+dbname)
                    self.changedb(dbname)
                else:
                    return

    def doquery(self,query,commit=False):
        #print self.sqldbname
        #print query
        cur = self.sqldb.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            print row[0]
        if commit:
            self.sqldb.commit()

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
        self.changedb(usedb)
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
        self.doquery(query)
        self.sqltablename = tablename
        return isvalid

    def run(self):
        self.setaction('starting run')
        self.status['status'] = 'running'
        #load a file
        while self.load_incoming_file():
            if self.filename.find("prod-edge") > -1:
                self.setaction("loading table "+self.sqltablename+" ("+self.sqldbname+")")
                self.movetofinish()
                continue
            columns = []
            self.setaction('importing file '+self.filename)
            self.status['progress']['total'] = str(self.filelength())
            self.status['progress']['current'] = 0
            for line in self.file:
                columns = line.split("\t")
                break
            if self.createtableandvalid(self.filename,columns):
                print "importing stuff"
                self.parselines(self.parseline)
            self.movetofinish()
            print "finishedfile"
        print "done"

    def parseline(self,line):
        zhash = hashlib.sha256(line).hexdigest()
        self.status['progress']['current'] += 1
        line = line.replace("\n","")
        line = line.replace('"',"''")
        data = line.split("\t")
        data.append(zhash)
        insertdata = '"'+'","'.join(data)+'"'
        self.doquery("INSERT IGNORE INTO "+self.sqltablename+" VALUES ( "+insertdata+" );",True)

def name():
    return str("sqlimport")

def status():
    return SQLImport.status

def runservice():
    return SQLImport()


class Book(peewee.Model):
    author = peewee.CharField()
    title = peewee.TextField()

    class Meta:
        database = sqldb