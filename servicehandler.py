import logging
import os
import SimpleHTTPServer
import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

ZRandom = "HELLO"

basepath = os.path.dirname(__file__)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging.basicConfig(filename=basepath+'/logs/handler.log',level=logging.DEBUG,formatter=formatter)
logger = logging.getLogger(__name__)

# Web Server

class MyHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        if self.path == "/status":
            self.wfile.write("THIS IS THE STATUS")
            self.wfile.write("Hello World!"+ZRandom)
        if self.path == "/mypage.html":
            self.wfile.write("<html><body>")
            self.wfile.write("Hello World!")
            self.wfile.write("</body></html>")
            return
        self.wfile.write('{"response":"error","error":"404"}')


#Web server
ServerClass  = BaseHTTPServer.HTTPServer
Protocol     = "HTTP/1.0"
ServerPort   = 8899

class Servicehandler():

    def __init__(self):
        print "INITIALISING"
        logger.info("JOEY JOJO")
        self.setup_webserver()

    def response(self):
        return "JOJO"

    def setup_webserver(self):
        logger.info("JOEY JOJO2")
        server_address = ('127.0.0.1', ServerPort)
        logger.info("JOEY JOJO3")
        MyHandler.protocol_version = Protocol
        logger.info("JOEY JOJO4")
        httpd = ServerClass(server_address, MyHandler)
        logger.info("JOEY JOJO5")
        sa = httpd.socket.getsockname()
        logger.info("JOEY JOJO6")
        logger.info("Serving HTTP on", sa[0], "port", sa[1], "...")
        logger.info("JOEY JOJO7")
        httpd.serve_forever()
        logger.info("JOEY JOJO8")
        logger.info("Still Serving HTTP on", sa[0], "port", sa[1], "...")
        MyHandler.response = "JOEY JOJO"