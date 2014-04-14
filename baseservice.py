#!/usr/bin/python
import importlib

import logging
import time
import os
import datetime
import MySQLdb
from pymongo import MongoClient
import hashlib
import pymongo

basepath = os.path.dirname(__file__)

#Logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/debug.log', level=logging.DEBUG, formatter=formatter)
logger = logging.getLogger(__name__)

#Base Service Class
class BaseService(object):

    # Status for the Rest API
    version = "development"
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

    #Attributes for Mongo
    mongo_enabled = False
    mongo_client = None
    mongo_db = None
    mongo_dbname = ""
    mongo_collection = None
    mongo_collectionname = ""

    #Attributes for SQL
    sql_enabled = False
    sql_host = 'localhost'
    sql_user = ''
    sql_pass = ''
    sql_dbname = ''
    sql_db = None
    sql_tablename = ''


    logformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
    backuplogformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
    file = None
    parser = None
    client = None
    db = None
    collection = None
    dbname = ""
    collectionname = ""
    hashfields = []
    servicename = 'baseservice'

    #Private
    filename = ""
    path_incoming = basepath+'/data/incoming'
    path_finished = basepath+'/data/finished'
    objects_added = 0

    # Starting the base service
    def initialize(self):
        self.log("info", "Starting service")
        if self.mongo_enabled:
            self.log("info", "Loading Mongo")
            self.connect_to_mongo(self.mongo_dbname,self.mongo_collectionname)
        if self.sql_enabled:
            self.log("info", "Loading MySQL")
            self.connect_to_sql(self.sql_dbname,True)
        self.log("info", "Running Setup")
        self.setaction('loading')
        self.setup()
        while True:
            self.log("info", "Run Loop")
            self.setaction('running')
            self.run()
            self.setaction('sleeping')
            time.sleep(60)

    # Connects to a Mongo Database
    def connect_to_mongo(self, db_name, collection_name):
        try:
            self.mongo_client = MongoClient('localhost', 27017)
            if db_name != "":
                self.mongo_db = self.mongo_client[db_name]
                if self.mongo_db:
                    self.mongo_dbname = db_name
                    if collection_name != "":
                        self.mongo_collection = self.db[collection_name]
                        if self.mongo_collection:
                            self.mongo_collectionname = collection_name
            return True
        except pymongo.errors.ConnectionFailure, e:
            self.log("error", "Could not connect to MongoDB: %s" % e)
        return False

    # Connects to a MySQL Database
    def connect_to_sql(self, db_name, force_reconnect=False, create_db=True):
        if (self.sql_dbname != db_name or force_reconnect) and db_name != "":
            try:
                self.sql_db = MySQLdb.connect(host=self.sql_host, user=self.sql_user, passwd=self.sql_pass, db=db_name)
                self.sql_dbname = db_name
                return True
            except Exception, e:
                if e[0] and create_db:
                    self.log("info", "Creating database "+db_name)
                    self.doquery("CREATE DATABASE "+db_name)
                    self.connect_to_sql(db_name)
                    return True
                else:
                    self.log("error", "Could not connect to MySQL: %s" % e)
                    return False
        return False


    # Need to override setup or bad
    def setup(self):
        raise Exception("You are required to create a setup method")

    # Need to override run or bad
    def run(self):
        raise Exception("You are required to create a run method")

    # Log to screen and to file
    def log(self, type, text):
        log = time.strftime('%Y_%m_%d %H_%M_%S')+" "+str(self.__class__.__name__)+": ("+type+") "+text
        print log
        logger.info(log)

    # Sets the action for the Rest API
    def setaction(self,theaction):
        if(theaction == 'stopped'):
            self.status['status'] = 'stopped'
        else:
            self.status['status'] = 'running'
        self.status['action'] = str(theaction)
        self.status['actiontime'] = time.strftime('%Y-%m-%d %H:%M:%S')


    def load_incoming_file(self):
        loaded = False
        for dirname, dirnames, filenames in os.walk(self.path_incoming):
            for filename in filenames:
                #open an incoming file
                self.filename = filename
                self.file = open(os.path.join(dirname, self.filename))
                if self.file:
                    loaded = True
        return loaded

    def movetofinish(self):
        try:
            self.file.close()
        except Exception:
            self.log("error","File is already closed")
        os.rename(os.path.join(self.path_incoming, self.filename),os.path.join(self.path_finished, time.strftime(
            '%Y_%m_%d')+"_"+self.filename))

    def parselines(self,func=None):
        with self.file as infile:
            for line in infile:
                func(line)

    def insert(self,obj):
        zhash = self.servicename+"_"
        for field in self.hashfields:
            if obj[field]:
                zhash += obj[field]+"_"
        zhash = hashlib.sha256(zhash).hexdigest()
        obj['hash'] = zhash
        done = self.collection.update({"hash":zhash},obj,True)
        if done['updatedExisting']:
            pass
        else:
            self.objects_added += 1

# Static methods

# Load a module
def loadModule(moduleName):
    mod = importlib.import_module('services.'+moduleName+'.service')
    datadirs = ['incoming','process','finished']
    mkdatadirsifneeded(moduleName)
    mklogifneeded(moduleName)
    return mod

# Create a list of directories within the base path
def mkdatadirsifneeded(moduleName):
    dirs = getdatafilepaths(moduleName)
    for dir in dirs:
        if not os.path.exists(dirs[dir]):
            try:
                os.makedirs(dirs[dir])
            except:
                print "Error: Could not create path "+dirs[dir]
                pass

# Create log file in the correct directory
def mklogifneeded(moduleName):
    with open(getlogfilepath(moduleName), 'a'):
        os.utime(getlogfilepath(moduleName), None)

# Get the log file path
def getlogfilepath(moduleName):
    basepath = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(basepath,'logs',moduleName+'.log')
    return path

# Get the data paths
def getdatafilepaths(moduleName):
    paths = {}
    basepath = os.path.dirname(os.path.abspath(__file__))
    paths['incoming'] = os.path.join(basepath,'data',moduleName,'incoming')
    paths['process'] = os.path.join(basepath,'data',moduleName,'process')
    paths['finished'] = os.path.join(basepath,'data',moduleName,'finished')
    return paths