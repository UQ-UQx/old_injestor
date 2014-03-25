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

    dbname = "logs"
    collectionname = "zz"
    #hashfields = ['remote_host', 'request_url', 'time_recieved_isoformat','request_first_line']
    #servicename = 'filelogparser'
    geoReader = None
    ipfield = 'ip'

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
        self.status['name'] = "IP To Country"
        self.log("info", "STARTING")
        self.initialize()

    def setup(self):
        self.geoReader = geoip2.database.Reader(basepath+'/lib/GeoIP2-Country.mmdb')


    def setaction(self,theaction):
        if(theaction == 'stopped'):
            self.status['status'] = 'stopped'
        else:
            self.status['status'] = 'running'
        self.status['action'] = str(theaction)
        self.status['actiontime'] = time.strftime('%Y-%m-%d %H:%M:%S')

    def run(self):
        self.status['status'] = 'running'
        for collection in self.db.collection_names():
            self.setaction('checking collections')
            self.collection = self.db[collection]
            if self.collection:
                self.setaction('checking collection '+collection)
                toupdates = self.collection.find({ self.ipfield : { '$exists' : True }, 'country' : { '$exists' :
                                                                                                           False }} )
                print "TOTAL ENTRIES MISSING FOR ("+self.ipfield+"): "+str(toupdates.count())+" FOR "+str(collection)
                self.status['progress']['total'] = str(toupdates.count())
                self.status['progress']['current'] = 0

                for toupdate in toupdates:
                    self.status['progress']['current'] += 1
                    if toupdate[self.ipfield] != '::1':
                        try:
                            country = self.geoReader.country(toupdate[self.ipfield])
                            isocountry = country.country.iso_code
                            self.collection.update({"_id": toupdate['_id']}, {"$set": {"country": isocountry}})
                        except AddressNotFoundError:
                            pass
                    else:
                        self.collection.update({"_id": ObjectId(toupdate['_id'])}, {"$set": {"country": ""}})



def name():
    return str("iptocountry")

def status():
    return Iptocountry.status

def runservice():
    return Iptocountry()