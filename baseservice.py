#!/usr/bin/python
import importlib

import logging
import time
import os
import urllib
import MySQLdb
from pymongo import MongoClient
import hashlib
import xml.etree.ElementTree as ET

try:
    import config
except ImportError:
    print "You do not have a config file, copy config.example.py to config.py"
    exit()
import courses

basepath = os.path.dirname(__file__)


#Base Service Class
class BaseService(object):

    def __init__(self):
        self.version = "development"
        # Status for the Rest API
        self.status = {
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
        self.logger = None

        #Attributes for Mongo
        self.mongo_enabled = False
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_dbname = ""
        self.mongo_collection = None
        self.mongo_collectionname = ""

        #Attributes for SQL
        self.sql_enabled = False
        self.sql_host = config.SQL_HOST
        self.sql_user = config.SQL_USERNAME
        self.sql_pass = config.SQL_PASSWORD
        self.sql_dbname = ''
        self.sql_db = None
        self.sql_tablename = ''

        #Loading from files inside data
        self.file = None
        self.filename = ""
        self.filepath = ""

        self.logformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""
        self.backuplogformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""

        self.parser = None
        self.client = None
        self.db = None
        self.collection = None
        self.dbname = ""
        self.collectionname = ""
        self.hashfields = []
        self.servicename = 'baseservice'
        self.loop = True

        #Private
        self.path_incoming = basepath + '/data/incoming'
        self.path_finished = basepath + '/data/finished'
        self.objects_added = 0

    # Sets the logging
    def setup_logging(self):
        #Logging
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        logging.basicConfig(filename=basepath + '/logs/' + self.servicename + '.log', level=logging.DEBUG,
                            formatter=formatter)
        self.logger = logging.getLogger(__name__)

    # Starting the base service
    def initialize(self):
        self.set_service_name()
        self.setup_logging()
        self.log("info", "Starting service")
        if self.mongo_enabled:
            self.log("info", "Loading Mongo")
            self.connect_to_mongo(self.mongo_dbname, self.mongo_collectionname)
        if self.sql_enabled:
            self.log("info", "Loading MySQL")
            self.connect_to_sql(self.sql_dbname, True)

        self.log("info", "Running Setup")
        self.setaction('loading')
        self.setup()
        while self.loop:
            self.log("info", "Run Loop")
            self.setaction('running')
            self.run()
            self.setaction('sleeping')
            self.status['lastawake'] = time.strftime('%Y-%m-%d %H:%M:%S')
            self.log("info", "Going to sleep")
            time.sleep(60)

    # Connects to a Mongo Database
    def connect_to_mongo(self, db_name="", collection_name=""):
        db_name = safe_name(db_name)
        try:
            if self.mongo_client is None:
                self.mongo_client = MongoClient('localhost', 27017)
            if db_name != "":
                self.mongo_db = self.mongo_client[db_name]
                if self.mongo_db:
                    self.mongo_dbname = db_name
                    if collection_name != "":
                        self.mongo_collection = self.mongo_db[collection_name]
                        if self.mongo_collection:
                            self.mongo_collectionname = collection_name
            return True
        except Exception, e:
            self.log("error", "Could not connect to MongoDB: %s" % e)
        return False

    # Connects to a MySQL Database
    def connect_to_sql(self, db_name, force_reconnect=False, create_db=True):
        db_name = safe_name(db_name)
        if (self.sql_dbname != db_name or force_reconnect) and db_name != "":
            try:
                self.sql_db = MySQLdb.connect(host=self.sql_host, user=self.sql_user, passwd=self.sql_pass, db=db_name)
                self.sql_dbname = db_name
                return True
            except Exception, e:
                if e[0] and create_db:
                    if self.sql_db is None:
                        self.sql_db = MySQLdb.connect(host=self.sql_host, user=self.sql_user, passwd=self.sql_pass,
                                                      db='mysql')
                    self.log("info", "Creating database " + db_name)
                    self.sql_query("CREATE DATABASE " + db_name)
                    self.connect_to_sql(db_name)
                    return True
                else:
                    self.log("error", "Could not connect to MySQL: %s" % e)
                    return False
        return False

    # Calls the dashboard reset function
    def reset_cache(self, type):
        if config.RESET_CACHE:
            #self.setaction('resetting cache for '+type)
            #response = urllib.urlopen(str(config.RESET_CACHE_URL+str(type)))
            #self.setaction('reset cache for '+type)
            #print response.read()
            pass

    # Gets the name of the service based on its class name
    def set_service_name(self):
        self.servicename = str(self.__class__.__name__).lower()

    # Need to override setup or bad
    def setup(self):
        raise Exception("You are required to create a setup method")

    # Need to override run or bad
    def run(self):
        raise Exception("You are required to create a run method")

    # Log to screen and to file
    def log(self, logtype, text):
        log = time.strftime('%Y_%m_%d %H_%M_%S') + " " + self.servicename + ": (" + logtype + ") " + text
        self.logger.info(log)
        print log

    # Sets the action for the Rest API
    def setaction(self, theaction):
        if theaction == 'stopped':
            self.status['status'] = 'stopped'
        else:
            self.status['status'] = 'running'
        self.status['action'] = str(theaction)
        self.status['actiontime'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # Loads an incoming file from the incoming data path for the module
    def load_incoming_file(self):
        filepaths = getdatafilepaths(self.servicename)
        loaded = False
        for dirname, dirnames, filenames in os.walk(filepaths['incoming']):
            for filename in filenames:
                #open an incoming file
                self.file = open(os.path.join(dirname, filename))
                self.filename = filename
                self.filepath = dirname
                if self.file:
                    loaded = True
        return loaded

    # Counts the number of lines within the open file
    def numlines(self):
        i = 0
        for i, l in enumerate(self.file):
            pass
        self.file.seek(0)
        return i + 1

    # Counts the number of files within incoming
    def numfiles(self):
        paths = getdatafilepaths(self.servicename)
        count = 0
        for dirname, dirnames, filenames in os.walk(paths['incoming']):
            for _ in filenames:
                count += 1
        return count
    
    # return the path of subsetbackup
    def get_backup_path(self):
        paths = getdatafilepaths(self.servicename)
        return paths['subsetbackup']     

    # Moves the open file from incoming to finished, including subdirectories
    def movetofinish(self, prepend_date=False):
        paths = getdatafilepaths(self.servicename)
        try:
            self.file.close()
        except IOError:
            self.log("error", "File is already closed")
        newfilename = self.filename
        finpath = self.filepath.replace("incoming", "finished", 1)
        if not os.path.exists(finpath):
            os.makedirs(finpath)
        if prepend_date:
            newfilename = time.strftime('%Y_%m_%d') + "_" + newfilename
        os.rename(os.path.join(self.filepath, self.filename), os.path.join(finpath, newfilename))
        if os.listdir(self.filepath) == [] and self.filepath != paths['incoming']:
            os.rmdir(self.filepath)

    # Loops through the open file and calls the supplied function
    def parselines(self, func=None):
        with self.file as infile:
            for line in infile:
                func(line)

    # Inserts an object into the Mongo Database
    def mongo_insert(self, obj):
        field_hash = self.servicename + "_"
        for field in self.hashfields:
            if obj[field]:
                field_hash += obj[field] + "_"
        field_hash = hashlib.sha256(field_hash).hexdigest()
        obj['hash'] = field_hash
        done = self.mongo_collection.update({"hash": field_hash}, obj, True)
        if done['updatedExisting']:
            pass
        else:
            self.objects_added += 1

    # Does a query on the active MySQL Database
    def sql_query(self, query, commit=False):
        cur = self.sql_db.cursor()
        cur.execute(query)
        for row in cur.fetchall():
            print row[0]
        if commit:
            self.sql_db.commit()

    # Unpacks an XML file into a python object
    def xml_unpack_file(self,filename):
        return self.xml_unpack(ET.parse(filename))

    # Unpacks an XML tree into a python object
    def xml_unpack(self,tree):
        return self.xml_unpackelement(tree.getroot())

    # Recursively turns XML into a nested object
    def xml_unpackelement(self,el):
        obj = {'children':[],'tag':el.tag}
        for attrib_name in el.attrib:
            obj[attrib_name] = el.attrib[attrib_name]
        count = 0
        for child in el:
            obj['children'].append(self.xml_unpackelement(child))
        return obj
    
    def get_course(self, course_id):
        if course_id in courses.EDX_DATABASES:
            return courses.EDX_DATABASES[course_id]
        return None

    def get_all_courses(self):
        course_dict = dict(courses.EDX_DATABASES)
        return course_dict



# Static methods
def safe_name(filename):
    return str(filename).replace('.','_')


# Load a module
def load_module(module_name):
    mod = importlib.import_module('services.' + module_name + '.service')
    mkdatadirsifneeded(module_name)
    mklogifneeded(module_name)
    return mod


# Create a list of directories within the base path
def mkdatadirsifneeded(module_name):
    dirs = getdatafilepaths(module_name)
    for the_dir in dirs:
        if not os.path.exists(dirs[the_dir]):
            try:
                os.makedirs(dirs[the_dir])
            except IOError:
                print "Error: Could not create path " + dirs[the_dir]
                pass


# Create log file in the correct directory
def mklogifneeded(module_name):
    with open(getlogfilepath(module_name), 'a'):
        os.utime(getlogfilepath(module_name), None)


# Get the log file path
def getlogfilepath(module_name):
    thebasepath = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(thebasepath, 'logs', module_name + '.log')
    return path


# Get the data paths
def getdatafilepaths(module_name):
    paths = {}
    thebasepath = os.path.dirname(os.path.abspath(__file__))
    paths['basedata'] = os.path.join(thebasepath, 'data', module_name)
    paths['incoming'] = os.path.join(thebasepath, 'data', module_name, 'incoming')
    paths['process'] = os.path.join(thebasepath, 'data', module_name, 'process')
    paths['finished'] = os.path.join(thebasepath, 'data', module_name, 'finished')

    if module_name == "extractsample":
        paths['subsetbackup'] = os.path.join(thebasepath, 'data', module_name, 'subsetbackup')

    return paths