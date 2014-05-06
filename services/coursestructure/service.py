#!/usr/bin/python

import os
import lxml
import time
import json
import pymongo
from datetime import datetime
from bson.objectid import ObjectId
import baseservice
from lxml import etree

import pprint

basepath = os.path.dirname(__file__)


class Coursestructure(baseservice.BaseService):
    inst = None

    def __init__(self):

        Coursestructure.inst = self
        super(Coursestructure, self).__init__()

        self.status['name'] = "Course Structure"
        self.mongo_enabled = False
        self.mongo_dbname = ""
        self.mongo_collectionname = ""

        self.mongo_files = []

        #Private
        self.initialize()

    def setup(self):
        #Get meta-data from info.json
        print "INITIALIZING"

    def run(self):
        filepaths = baseservice.getdatafilepaths(self.servicename)
        loaded = False
        for dirname, dirnames, filenames in os.walk(filepaths['incoming']):
            innerpath = dirname.replace(filepaths['incoming'], "")
            dircount = innerpath.count('/')
            if dircount == 0:
                #for files not in a directory
                for file in filenames:
                    incpath = os.path.join(dirname, file)
                    finpath = os.path.join(filepaths['finished'], file)
                    os.rename(incpath, finpath)
            if dircount == 1:
                self.parsecourse(dirname)
        return loaded
        print 'dongs'

    def parsecourse(self, path):
        coursename = os.path.basename(os.path.normpath(path))
        coursesplit = coursename.split("-")
        term = coursesplit[-1]
        #Build the XML
        course = {}
        #Parse the course
        coursefile = os.path.join(path, 'course', term + '.xml')
        course = self.xml_unpack_file(coursefile)
        print course
        print "Parsing " + coursename + " at " + path
        exit(0)


def name():
    return str("coursestructure")


def status():
    return Coursestructure.inst.status


def runservice():
    return Coursestructure()


class objectJSONEncoder(json.JSONEncoder):
    """A specialized JSON encoder that can handle simple lxml objectify types
      >>> from lxml import objectify
      >>> obj = objectify.fromstring("<Book><price>1.50</price><author>W. Shakespeare</author></Book>")
      >>> objectJSONEncoder().encode(obj)
      '{"price": 1.5, "author": "W. Shakespeare"}'
    """

    def default(self, o):
        if isinstance(o, lxml.objectify.IntElement):
            return int(o)
        if isinstance(o, lxml.objectify.NumberElement) or isinstance(o, lxml.objectify.FloatElement):
            return float(o)
        if isinstance(o, lxml.objectify.ObjectifiedDataElement):
            return str(o)
        if hasattr(o, '__dict__'):
            #For objects with a __dict__, return the encoding of the __dict__
            return o.__dict__
        return json.JSONEncoder.default(self, o)