#!/usr/bin/env python
import pdb

def test(n):
    pdb.set_trace()
    print "This is the first step"
    if isinstance(n, int):
        print "You provided an integer"
    elif isinstance(n, str):
        print "You provided a string"
    elif isinstance(n, dict):
        print "You provided a dictionary"
    elif isinstance(n, tuple):
        print "You provided a tuple"
