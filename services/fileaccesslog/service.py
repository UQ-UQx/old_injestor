#!/usr/bin/python

import os
import time
import baseservice
import apache_log_parser

basepath = os.path.dirname(__file__)

class Example(baseservice.BaseService):

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
    dbname = 'logs'
    collectionname = 'online_access_logs'

    logformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
    parser = None

    def __init__(self):
        self.status['name'] = "Access Logs Parser"
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
        self.parser = apache_log_parser.make_parser(self.logformat)
        pass

    def run(self):
        print "Running"
        self.setaction('test running')
        self.status['status'] = 'running'
        while self.load_incoming_file():
            self.setaction("loading access log "+self.filename)
            self.status['progress']['total'] = str(self.filelength())
            self.status['progress']['current'] = 0
            self.parselines(self.parseline)
            self.movetofinish()

    def parseline(self,line):
        try:
            log_line_data = self.parser(line)
            self.insert(log_line_data)
            self.status['progress']['current'] += 1
        except Exception, e:
            print "BAD LINE"+str(e)


def name():
    return str("example")

def status():
    return Example.status

def runservice():
    return Example()