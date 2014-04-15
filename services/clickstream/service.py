#!/usr/bin/python

import os
import time
import baseservice

basepath = os.path.dirname(__file__)

class Clickstream(baseservice.BaseService):

    inst = None

    def __init__(self):

        Clickstream.inst = self
        super(Clickstream, self).__init__()

        self.version = "development"
        self.dbname = "logs"
        self.collectionname = "clickstream"
        self.tmp_collectionname = "clickstream_tmp"
        self.status['name'] = "Clickstream"
        self.initialize()

        self.log("info", "STARTING CLICKSTREAM")

    def setup(self):
        self.log("info", "SETUP")

    def run(self):
        self.setaction('starting run')
        self.status['status'] = 'running'
        #load a file
        self.status['progress']['total'] = str(self.numfiles())
        self.status['progress']['current'] = 1
        while self.load_incoming_file():
            valid = False
            try:
                if self.filename.find('.log') != -1:
                    valid = True
                if self.filepath.find('-edge-') != -1:
                    valid = False
            except:
                print "BAD"
                pass
            if valid:
                print "loading clickstream file "+self.filepath+"/"+self.filename
                cmd = "mongoimport --db "+self.dbname+" --collection "+self.tmp_collectionname+" < "+self.filepath+"/"+self.filename
                os.system(cmd)
                print "Importing "+self.filepath+"/"+self.filename+" "+str(self.status['progress']['current'])+" out of "+str(self.status['progress']['total'])
            self.movetofinish()
            self.status['progress']['current'] += 1
        mongoremove = "mongo "+self.dbname+" --eval \"db."+self.collectionname+".remove()\""
        os.system(mongoremove)
        mongomove = "mongo "+self.dbname+" --eval \"db."+self.tmp_collectionname+".renameCollection('"+self.collectionname+"')\""
        os.system(mongomove)

def name():
    return str("clickstream")

def status():
    return Clickstream.inst.status

def runservice():
    return Clickstream()