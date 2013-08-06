#!/usr/bin/env python
from boto.s3.connection import S3Connection
import csv
import time
import sys

if __name__ == '__main__':
    if sys.argv[1].isdigit():
        conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
        realtime = conn.get_bucket('fbrealtime')
        days = time.time() - (86400 * int(sys.argv[1]))
        for key in realtime.list():
 	    if int(key.key) < days:
		realtime.delete_key(key)
    else:
        print "Error"
