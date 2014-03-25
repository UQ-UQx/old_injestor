#!/usr/bin/python

import os
import baseservice

basepath = os.path.dirname(__file__)

class Clickstream(baseservice.BaseService):

    status = {'status':'stopped','action':'stopped','progress':{'current':'0','total':'0'},'lastawake':'0000-00-00 '
                                                                                                       '00:00:00'}

    def __init__(self):
        self.initialize()

    def setup(self):
        pass

    def run(self):
        self.status['status'] = 'running'


def name():
    return str("Example Service")

def status():
    return Clickstream.status

def runservice():
    return Clickstream()