#!/usr/bin/python
import importlib

import sys
import baseservice

if len(sys.argv) < 2:
    print "Error: Please supply a module name"
else:
    moduleName = sys.argv[1]
    try:
        mod = baseservice.loadModule(moduleName)
        print "Test run of service: "+moduleName
        print mod.runservice()
        print "Finished test run"
    except:
        print "Error: Invalid service name: "+moduleName