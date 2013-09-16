#!usr/bin/env python
import csv
import os

"""
ubuntu@ip-10-114-99-75:~$ echo $XDG_SESSION_COOKIE
a7689822e621ef513944eaca5175d76c-1379344811.890203-1577907683
"""

def xor_cipher(string):
    key = os.environ['XDG_SESSION_COOKIE'][8]
    new = ''
    for i in string:
        new += chr(ord(i) ^ ord(key))
    return new
