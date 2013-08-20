#!usr/bin/env python
import csv
import os

def xor_cipher(string):
    key = os.environ['XDG_SESSION_COOKIE'][8]
    new = ''
    for i in string:
        new += chr(ord(i) ^ ord(key))
    return new
