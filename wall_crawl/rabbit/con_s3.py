#!/usr/bin/env python
import csv
from xor import xor_cipher

def connect_s3():
    from boto.s3.connection import S3Connection
    f = open('creds.txt', 'r')
    f1 = f.read().split('\n')
    f.close()
    first = f1[0]
    second = f1[1]
    first = xor_cipher(first)
    second = xor_cipher(second)
    conn = S3Connection(first, second)
    return conn
