from SocketServer import ThreadingMixIn
import json
import logging
import os
import SimpleHTTPServer
import BaseHTTPServer
import threading
from time import sleep
import baseservice
import time
import config

basepath = os.path.dirname(__file__)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/handler.log', level=logging.DEBUG, formatter=formatter)
logger = logging.getLogger(__name__)

#Web server
ServerClass = BaseHTTPServer.HTTPServer
Protocol = "HTTP/1.0"
ServerPort = 8850


def log(message):
    mlog = time.strftime('%Y_%m_%d %H_%M_%S') + " " + "Service Handler" + ": (info) " + message
    print mlog
    logger.info(mlog)


class ServiceLoader():

    servicethreads = []
    servicemodules = []

    def __init__(self):
        log("Starting Service Loader")
        self.autoload()

    def autoload(self):
        servicespath = os.path.join(basepath, 'services')
        for servicename in os.listdir(servicespath):
            if servicename not in config.IGNORE_SERVICES:
                servicepath = os.path.join(servicespath, servicename, 'service.py')
                if os.path.exists(servicepath):
                    log("Starting module "+servicename)
                    servicemodule = baseservice.load_module(servicename)
                    servicethread = threading.Thread(target=servicemodule.runservice)
                    servicethread.start()
                    self.servicethreads.append(servicethread)
                    self.servicemodules.append(servicemodule)


class ResponseHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    response = 0
    servicehandler = None

    def runmodule(self, modulename, meta):
        servicespath = os.path.join(basepath, 'services')
        servicename = modulename
        servicepath = os.path.join(servicespath, modulename, 'service.py')
        if os.path.exists(servicepath):
            log("Starting once-off module "+servicename)
            servicemodule = baseservice.load_module(servicename)
            print meta
            servicethread = threading.Thread(target=servicemodule.runservice, args=meta)
            servicethread.start()

    def do_GET(self):
        response = {}
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if self.path == "/status":
            status = {}
            for sv in ServiceLoader.servicemodules:
                status[sv.name()] = sv.status()
            response['response'] = status
        elif "/run/" in self.path:
            info = self.path.split("/")
            service = info[2]
            meta = info[3:]
            self.runmodule(service, meta)
            response['response'] = "starting "+service
        else:
            response['response'] = "error"
            response['statuscode'] = 404
        self.wfile.write(json.dumps(response))


class ThreadedHTTPServer(ThreadingMixIn, BaseHTTPServer.HTTPServer):

    allow_reuse_address = True

    def shutdown(self):
        self.socket.close()
        BaseHTTPServer.HTTPServer.shutdown(self)


class Servicehandler():

    server = None
    server_thread = None
    service_loader = None

    def __init__(self):
        self.service_loader = ServiceLoader()
        self.setup_webserver()

    def setup_webserver(self):
        ResponseHandler.servicehandler = self
        server_address = ('0.0.0.0', ServerPort)
        ResponseHandler.protocol_version = Protocol
        self.server = ThreadedHTTPServer(server_address, ResponseHandler)
        log("Starting Web Server")
        self.start_webserver()
        log("Sleeping Main Thread")
        self.sleepmainthread()

    def sleepmainthread(self):
        sleep(2)
        self.sleepmainthread()

    def start_webserver(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()