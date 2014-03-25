#!/usr/bin/python

import logging
import time
import os
import datetime
from pymongo import MongoClient
import hashlib

basepath = os.path.dirname(__file__)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/debug.log',level=logging.DEBUG,formatter=formatter)
logger = logging.getLogger(__name__)

class BaseService(object):

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

    def initialize(self):
        self.connect_to_mongo()
        self.setup()
        while True:
            self.run()
            self.status['action'] = 'sleeping'
            self.status['lastawake'] = time.strftime('%Y-%m-%d %H:%M:%S')
            time.sleep(600)

    def setup(self):
        raise Exception("You are required to create a setup method")

    def run(self):
        raise Exception("You are required to create a run method")

    def log(self, type, text):
        print time.strftime('%Y_%m_%d %H_%M_%S')+" "+type+": "+text
        logger.info(text)

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

    def connect_to_mongo(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[self.dbname]
        if self.db:
            self.collection = self.db[self.collectionname]
            if self.collection:
                return True
        return False

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