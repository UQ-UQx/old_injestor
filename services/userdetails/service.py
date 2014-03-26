#!/usr/bin/python

import os
import time
import baseservice

basepath = os.path.dirname(__file__)

class Userdetails(baseservice.BaseService):

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
        self.status['name'] = "User Details"
        self.initialize()

    def setaction(self,theaction):
        if(theaction == 'stopped'):
            self.status['status'] = 'stopped'
        else:
            self.status['status'] = 'running'
        self.status['action'] = str(theaction)
        self.status['actiontime'] = time.strftime('%Y-%m-%d %H:%M:%S')

    def setup(self):
        pass

    def run(self):
        self.setaction('test running')
        self.status['status'] = 'running'

def name():
    return str("userdetails")

def status():
    return Userdetails.status

def runservice():
    return Userdetails()