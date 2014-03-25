#!/usr/bin/python

import os
import time
import baseservice

basepath = os.path.dirname(__file__)

class Clickstream(baseservice.BaseService):

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
        self.setaction('running')
        self.log("info", "RUNNING")

def name():
    return str("clickstream")

def status():
    return Clickstream.status

def runservice():
    return Clickstream()