#!/usr/bin/env python
import logging
from time import sleep
import sys
import traceback
from base.daemon import Daemon
import os
import time

import sys
from servicehandler import Servicehandler
import traceback

basepath = os.path.dirname(__file__)

#Logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/debug.log', level=logging.DEBUG, formatter=formatter)
logger = logging.getLogger(__name__)

def filelog(type, value, tb):
    error = "Fatal Error"+"\n\t"+'Type: '+str(type)+"\n\t"+'Value: '+str(value)+"\n\t"+'Traceback: '+str(tb)
    logger.error(error)
    print error

parser = None

parser = Servicehandler()