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

class UQXParser(Daemon):

    def run(self):
        handler = Servicehandler()


# Daemon Methods

if __name__ == "__main__":
    sys.excepthook = filelog
    daemon = UQXParser('/tmp/daemon-example.pid')
    if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                logger.info(time.strftime("Service: "+'%Y_%m_%d %H_%M_%S')+" Starting Daemon")
                daemon.start()
            elif 'stop' == sys.argv[1]:
                logger.info(time.strftime("Service: "+'%Y_%m_%d %H_%M_%S')+" Stopping Daemon")
                daemon.stop()
            elif 'restart' == sys.argv[1]:
                logger.info(time.strftime("Service: "+'%Y_%m_%d %H_%M_%S')+" Restarting Daemon")
                daemon.restart()
            elif 'status' == sys.argv[1]:
                if daemon.status():
                    status = " Daemon Running"
                else:
                    status = " Daemon Stopped"
                print status
                logger.info(time.strftime("Service: "+'%Y_%m_%d %H_%M_%S')+" "+status)
            else:
                print "Unknown command"
                sys.exit(2)
            sys.exit(0)
    else:
            print "usage: %s start|stop|restart|status" % sys.argv[0]
            sys.exit(2)

parser = UQXParser()