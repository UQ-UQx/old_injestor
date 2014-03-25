#!/usr/bin/python

import os
import baseservice

basepath = os.path.dirname(__file__)

class Clickstream(baseservice.BaseService):

    status = {'status':'stopped','action':'stopped','progress':{'current':'0','total':'0'},'lastawake':'0000-00-00 '
                                                                                                       '00:00:00'}

    def __init__(self):
        self.log("info", "STARTING CLICKSTREAM")
        self.initialize()

    def setup(self):
        self.log("info", "SETUP")

    def run(self):
        self.status['status'] = 'running'
        self.log("info", "RUNNING")


def name():
    return str("Clickstream")

def status():
    return Clickstream.status

def runservice():
    return Clickstream()