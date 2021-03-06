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

        self.outputdir = 'www/course_structure'

        #Private
        self.initialize()

    def setup(self):
        #Get meta-data from info.json
        pass

    def run(self):
        self.setaction('Parsing courses')
        self.status['status'] = 'running'
        filepaths = baseservice.getdatafilepaths(self.servicename)
        loaded = False
        for dirname, dirnames, filenames in os.walk(filepaths['incoming']):
            innerpath = dirname.replace(filepaths['incoming'], "")
            dircount = innerpath.count('/')
            if dircount == 0:
                #for files not in a directory
                for filename in filenames:
                    self.movefiletofinish(dirname,filename)
            if dircount == 1:
                self.parsecourse(dirname)
                self.movedirtofinish(dirname)
        return loaded

    def movefiletofinish(self, dirname, filename):
        incpath = os.path.join(dirname, filename)
        finpath = incpath.replace("incoming", "finished", 1)
        os.rename(incpath, finpath)

    def movedirtofinish(self, dirname):
        incpath = dirname
        finpath = incpath.replace("incoming", "finished", 1)
        print incpath
        print finpath
        os.rename(incpath, finpath)


    def parsecourse(self, path):
        coursename = os.path.basename(os.path.normpath(path))
        self.setaction("Parsing course: "+coursename)
        self.status['progress']['total'] = 10
        self.status['progress']['current'] = 0
        coursesplit = coursename.split("-")
        term = coursesplit[-1]
        #Build the XML
        course = {}
        #Parse the course
        coursefile = os.path.join(path, 'course', term + '.xml')
        course = self.xml_unpack_file(coursefile)
        self.status['progress']['current'] = 4
        course = self.add_linked_file_xml(path,course)
        policyfileurl = os.path.join(path, 'policies', term, 'policy.json')
        policyfile=open(policyfileurl).read()
        policydata = json.loads(policyfile)
        course['policy'] = policydata
        self.status['progress']['current'] = 8
        f = open(self.outputdir+'/'+coursename+'.json', 'w+')
        self.status['progress']['current'] = 9
        f.write(json.dumps(course))
        self.status['progress']['current'] = 10

    def print_course(self,course):
        pp = pprint.PrettyPrinter(indent=4)
        for chapter in course['children']:
            print 'Chapter: '+chapter['display_name']
            for sequence in chapter['children']:
                print '\tSequence: '+sequence['display_name']
                for vertical in sequence['children']:
                    print '\t\tVertical: '+vertical['display_name']
                    for something in vertical['children']:
                        display_name = 'Unknown'
                        if 'display_name' in something:
                            display_name = something['display_name']
                        print '\t\t\t'+something['tag']+': '+display_name
                        print something

    def add_linked_file_xml(self, basepath, xml_object):
        if len(xml_object['children']) > 0:
            index = 0
            for child in xml_object['children']:
                if len(child['children']) == 0 and 'url_name' in child:
                    child_path = os.path.join(basepath,child['tag'],child['url_name']+'.xml')
                    if os.path.isfile(child_path):
                        child_obj = (self.xml_unpack_file(child_path))
                        for key in child_obj:
                            child[key] = child_obj[key]
                        xml_object['children'][index] = self.add_linked_file_xml(basepath,child)
                index += 1
        return xml_object


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