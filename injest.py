#!/usr/bin/env python
import logging
import os
from servicehandler import Servicehandler

basepath = os.path.dirname(__file__)

#Logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/debug.log', level=logging.DEBUG, formatter=formatter)
logger = logging.getLogger(__name__)


def filelog(the_type, value, tb):
    error = "Fatal Error"+"\n\t"+'Type: '+str(the_type)+"\n\t"+'Value: '+str(value)+"\n\t"+'Traceback: '+str(tb)
    logger.error(error)
    print error

parser = Servicehandler()