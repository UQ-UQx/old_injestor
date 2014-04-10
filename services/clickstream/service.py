#!/usr/bin/python

import os
import time
import baseservice

basepath = os.path.dirname(__file__)

class Clickstream(baseservice.BaseService):

    dbname = "logs"
    collectionname = "clickstream"
    tmp_collectionname = "clickstream_tmp"

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
        self.status['name'] = "Clickstream"
        self.log("info", "STARTING CLICKSTREAM")
        self.initialize()

    def setaction(self,theaction):
        if(theaction == 'stopped'):
            self.status['status'] = 'stopped'
        else:
            self.status['status'] = 'running'
        self.status['action'] = str(theaction)
        self.status['actiontime'] = time.strftime('%Y-%m-%d %H:%M:%S')

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
                cmd = "mongoimport --db "+self.dbname+" --collection "+self.tmp_collectionname+" < "+self.filepath+"/"+self.filename
                os.system(cmd)
                print "Importing "+self.filepath+"/"+self.filename+" "+str(self.status['progress']['current'])+" out of "+str(self.status['progress']['total'])
            self.movetofinish(date=False)
            self.status['progress']['current'] += 1
        mongoremove = "mongo "+self.dbname+" --eval \"db."+self.collectionname+".remove()\""
        os.system(mongoremove)
        mongomove = "mongo "+self.dbname+" --eval \"db."+self.tmp_collectionname+".renameCollection('"+self.collectionname+"')\""
        os.system(mongomove)

def name():
    return str("clickstream")

def status():
    return Clickstream.status

def runservice():
    return Clickstream()