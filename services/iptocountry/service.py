#!/usr/bin/python

import time
import apache_log_parser
import baseservice
from bson import ObjectId
import geoip2
from geoip2 import database
from geoip2.errors import *
import os
from pymongo import *

basepath = os.path.dirname(__file__)

class Iptocountry(baseservice.BaseService):

    inst = None

    def __init__(self):

        Iptocountry.inst = self
        super(Iptocountry, self).__init__()

        self.version = "testing"
        self.mongo_dbname = 'logs'
        self.mongo_enabled = True

        self.geo_reader = None
        self.ipfield = 'ip'
        self.initialize()

    def setup(self):
        self.status['name'] = "IP To Country"
        self.geo_reader = geoip2.database.Reader(basepath+'/lib/GeoIP2-Country.mmdb')

    def run(self):
        for collection in self.mongo_db.collection_names():
            self.setaction('checking collections')
            self.mongo_collection = self.mongo_db[collection]
            if self.mongo_collection:
                self.setaction('checking collection '+collection)
                toupdates = self.mongo_collection.find({ self.ipfield : { '$exists' : True }, 'country' : { '$exists' :
                                                                                                           False }} )
                self.log('info',"checked collection ("+self.ipfield+"): "+str(toupdates.count())+" FOR "+str(
                    collection))
                self.status['progress']['total'] = str(toupdates.count())
                self.status['progress']['current'] = 0

                for toupdate in toupdates:
                    self.status['progress']['current'] += 1
                    if toupdate[self.ipfield] != '::1':
                        try:
                            country = self.geo_reader.country(toupdate[self.ipfield])
                            isocountry = country.country.iso_code
                            self.mongo_collection.update({"_id": toupdate['_id']}, {"$set": {"country": isocountry}})
                        except AddressNotFoundError:
                            pass
                    else:
                        self.mongo_collection.update({"_id": ObjectId(toupdate['_id'])}, {"$set": {"country": ""}})


def name():
    return str("iptocountry")

def status():
    return Iptocountry.inst.status

def runservice():
    return Iptocountry()