from SocketServer import ThreadingMixIn
import importlib
import json
import logging
import os
import SimpleHTTPServer
import BaseHTTPServer
import threading
from time import sleep
import baseservice

ZRandom = "HELLO"

basepath = os.path.dirname(__file__)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/handler.log',level=logging.DEBUG,formatter=formatter)
logger = logging.getLogger(__name__)

#Web server
ServerClass  = BaseHTTPServer.HTTPServer
Protocol     = "HTTP/1.0"
ServerPort   = 8850
TestCounter  = 0

class ServiceLoader():

    servicethreads = []
    servicemodules = []

    def __init__(self):
        logger.info("Starting Service Loader")
        self.autoload()

    def autoload(self):
        servicespath = os.path.join(basepath,'services')
        for servicename in os.listdir(servicespath):
            if servicename != 'extractsample':
                servicepath = os.path.join(servicespath,servicename,'service.py')
                if(os.path.exists(servicepath)):
                    logger.info("Starting module "+servicename)
                    servicemodule = baseservice.load_module(servicename)
                    servicethread = threading.Thread(target=servicemodule.runservice)
                    servicethread.start()
                    self.servicethreads.append(servicethread)
                    self.servicemodules.append(servicemodule)


class ResponseHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    response = 0
    servicehandler = None

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
        elif self.path == "/mypage.html":
            self.wfile.write("<html><body>")
            self.wfile.write("Hello World!")
            self.wfile.write("</body></html>")
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
        logger.info("Starting Web Server")
        self.start_webserver()
        logger.info("Sleeping main thread")
        self.sleepmainthread()

    def sleepmainthread(self):
        #logger.info("MAIN THREAD")
        sleep(2)
        self.sleepmainthread()

    def start_webserver(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
    #
    # def wait_webserver(self):
    #     self.server_thread.join()
    #
    # def stop_webserver(self):
    #     self.server.shutdown()
    #     self.wait_webserver()