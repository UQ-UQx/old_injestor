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
        self.mongo_dbname = "logs"
        self.mongo_collectionname = "clickstream"
        self.status['name'] = "Clickstream"
        self.initialize()

        self.log("info", "STARTING CLICKSTREAM")

    def setup(self):
        self.log("info", "SETUP")

    def run(self):
        self.setaction('starting run')
        self.status['status'] = 'running'
        #find out what the last file creation dates were
        maxdates = {}
        paths = baseservice.getdatafilepaths(self.servicename)
        for dirname, dirnames, filenames in os.walk(paths['incoming']):
            for filename in filenames:
                if self.validclickstreamlog(dirname, filename):
                    filetime = self.filenametodate(filename)
                    if dirname not in maxdates:
                        maxdates[dirname] = filetime
                    if(filetime > maxdates[dirname]):
                        maxdates[dirname] = filetime

        #load a file
        self.status['progress']['total'] = str(self.numfiles() + 20)
        self.status['progress']['current'] = 1
        self.set_indexes()
        while self.load_incoming_file():
            if self.validclickstreamlog(self.filepath,self.filename):
                self.setaction('importing log '+self.filename)
                self.set_indexes()
                #Check whether its a valid date
                filedate = self.filenametodate(self.filename)
                if filedate == maxdates[self.filepath]:
                    print "THIS IS MAX DATE, NEED TO IGNORE IT"
                    pass
                elif not self.checkwritten(self.filepath,self.filename):
                    cmd = "mongoimport --db "+self.mongo_dbname+" --collection "+self.mongo_collectionname+" < "+self\
                        .filepath+"/"+self.filename
                    os.system(cmd)
                    self.addwritten(self.filepath,self.filename)
                    print "Importing "+self.filepath+"/"+self.filename+" "+str(self.status['progress']['current'])+" out of "+str(self.status['progress']['total'])
                else:
                    print "Already done, ignoring"
            self.status['progress']['current'] += 1
            self.movetofinish()
        self.reset_cache('clickstream')
        self.status['progress']['current'] += 20

    def set_indexes(self):
        print "Setting index for countries"
        cmd = "mongo --eval \"db['logs'].clickstream.ensureIndex({country:1})\""
        os.system(cmd)
        print "Setting index for event-course"
        cmd = "mongo --eval \"db['logs'].clickstream.ensureIndex( {event_type: 1,'context.course_id': 1} )\""
        os.system(cmd)
        print "Setting index for course"
        cmd = "mongo --eval \"db['logs'].clickstream.ensureIndex( {'context.course_id': 1} )\""
        os.system(cmd)

    def checkwritten(self, filepath, filename):
        paths = baseservice.getdatafilepaths(self.servicename)
        existingpath = False
        sig = filepath+"/"+filename
        with open(os.path.join(paths['basedata'], 'injested.txt'), "r") as myfile:
            for line in myfile:
                sngline = line.replace("\n","")
                if sngline == sig:
                    existingpath = True
                    break
        return existingpath

    def addwritten(self, filepath, filename):
        paths = baseservice.getdatafilepaths(self.servicename)
        with open(os.path.join(paths['basedata'], 'injested.txt'), "a") as myfile:
            myfile.write(filepath+"/"+filename+"\n")

    def filenametodate(self, filename):
        date = filename.replace("_UQx.log", "")
        date = time.strptime(date, "%Y-%m-%d")
        return date

    def validclickstreamlog(self, filepath, filename):
        valid = False
        try:
            if filename.find('.log') != -1:
                valid = True
            if filepath.find('-edge-') != -1:
                valid = False
        except:
            pass
        return valid

def name():
    return str("clickstream")

def status():
    return Clickstream.inst.status

def runservice():
    return Clickstream()