#!/usr/bin/python
import importlib

import sys
import baseservice


if len(sys.argv) < 2:
    print "Error: Please supply a module name"
else:
    moduleName = sys.argv[1]
    #try:
    mod = baseservice.load_module(moduleName)
    print "Test run of service: "+moduleName
    if moduleName == "extractsample":
        print mod.runservice(sys.argv[2], sys.argv[3], sys.argv[4])
    elif moduleName == "personcourse":
        print mod.runservice(sys.argv[2])
    else:
        print mod.runservice()
    print "Finished test run"
