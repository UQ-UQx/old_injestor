#!/usr/bin/python
from datetime import datetime

import time
import apache_log_parser
import baseservice
from bson import ObjectId
import geoip2
from geoip2 import database
from geoip2.errors import *
import os
from pymongo import *
import dateutil.parser

basepath = os.path.dirname(__file__)

class Timefinder(baseservice.BaseService):

    inst = None

    def __init__(self):

        Timefinder.inst = self
        super(Timefinder, self).__init__()

        self.version = "testing"
        self.mongo_dbname = 'logs'
        self.mongo_enabled = True

        self.geo_reader = None
        self.timefield = 'time'
        self.initialize()

    def setup(self):
        self.status['name'] = "Time Finder"

    def run(self):
        for collection in self.mongo_db.collection_names():
            self.setaction('checking collections')
            self.mongo_collection = self.mongo_db[collection]
            if self.mongo_collection:
                self.setaction('checking collection '+collection)
                toupdates = self.mongo_collection.find({ self.timefield : { '$exists' : True }, 'time_date' : { '$exists' :
                                                                                                           False }} )
                self.log('info',"checked collection ("+self.timefield+"): "+str(toupdates.count())+" FOR "+str(
                    collection))
                self.status['progress']['total'] = str(toupdates.count())
                self.status['progress']['current'] = 0

                for toupdate in toupdates:
                    self.status['progress']['current'] += 1
                    self.mongo_collection.update({"_id": ObjectId(toupdate['_id'])}, {"$set": {"time_date": dateutil.parser.parse(toupdate['time'])}})

def name():
    return str("timefinder")

def status():
    return Timefinder.inst.status

def runservice():
    return Timefinder()