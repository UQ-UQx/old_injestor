#!/usr/bin/python

import os
import time
import json
import pymongo
from datetime import datetime
from bson.objectid import ObjectId
import baseservice

import pprint

basepath = os.path.dirname(__file__)


class Mongoimport(baseservice.BaseService):

    mongo_enabled = False
    mongo_dbname = ""
    mongo_collectionname = ""
    status = {
        'name': 'unknown',
        'status': 'stopped',
        'action': 'stopped',
        'actiontime': '000-00-00 00:00:00',
        'progress': {
            'current': '0',
            'total': '0'
        },
        'lastawake': '0000-00-00 00:00:00'
    }

    mongo_files = []

    #Private
    info_filename = "info.json"
    path_info = os.path.join(basepath, info_filename)

    def __init__(self):
        self.status['name'] = "Mongo Importer"
        self.initialize()

    def setup(self):
        #Get meta-data from info.json
        print "Connected"
        self.parse_info()
        if self.mongo_dbname:
            print "A"
            #self.mongo_db = self.mongo_client[self.mongo_dbname]
            print "B"
            print "Connected"
        else:
            pass

    def parse_info(self):
        try:
            info_file = open(self.path_info)
        except IOError:
            print "Error: File does not appear to exist."
        else:
            info_data = json.load(info_file)
            self.mongo_dbname = info_data["topic"]
            for course in info_data["courses"]:
                course_id = "UQx-" + course["course_code"] + "-" + course["platform"]
                file_name = course_id + '.mongo'
            #    self.mongo_collections.append(course_id)
                self.mongo_files.append(file_name)

    def run(self):
        print self.mongo_dbname
        self.setaction('test running')
        #ToDo: How to take files from qcloud and put into our incoming folder?
        ###
        #load a file
        while self.load_incoming_file():
            print self.filename
            if self.filename in self.mongo_files:
                if self.filename.endswith(".mongo"):
                    self.connect_to_mongo(self.mongo_dbname, self.filename[:-6])
                else:
                    pass
                self.setaction("loading file " + self.filename + " to " + self.mongo_dbname)
                #ToDo: injest into mongodb
                ###
                self.status['progress']['total'] = self.numlines()
                self.status['progress']['current'] = 0
                for line in self.file:
                    document = json.loads(line)
                    if '_id' in document:
                        self.insert_with_id(document)
                        #print "objects added %d" % self.objects_added
                    else:
                        #ToDo: point unique fields
                        self.mongo_insert(self.format_document(document))
                    self.status['progress']['current'] += 1
                    #print "status is "+str(self.status['progress']['current'])+' out of '+str(self.status['progress'][
                    #    'total'])
                self.movetofinish()
            else:
                self.setaction("loading file " + self.filename + ". It is not in our list.")
                self.movetofinish()

    @staticmethod
    def format_document(document):
        for key, item in document.items():
            #replace $ in key to _ and . in key to -
            new_key = key.replace("$", "_").replace(".", '"-')
            document[new_key] = document.pop(key)
            #process "$oid" and "$date" in item
            if type(item) == type({}):
                if "$oid" in item:
                    document[key] = ObjectId(str(item["$oid"]))
                if "$date" in item:
                    document[key] = datetime.utcfromtimestamp(item["$date"]/1e3)
        return document

    def insert_with_id(self, document):
        document = self.format_document(document)
        doc_id = document.pop('_id')
        #print doc_id
        done = self.mongo_collection.update({"_id": doc_id}, {"$set": document}, upsert=True)
        if done['updatedExisting']:
            pass
        else:
            self.objects_added += 1


def name():
    return str("mongoimport")


def status():
    return Mongoimport.status


def runservice():
    return Mongoimport()