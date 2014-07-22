import sys
import urllib
import string
import csv
import unicodecsv

import time
import datetime
from datetime import datetime, date, time
from pprint import pprint

from types import *

import re

if len(sys.argv) == 3:
    datafile = sys.argv[1]
    outfile = sys.argv[2]
else:
    sys.exit("Error - need filenames to read from and write to")



outfile = open(outfile, 'wt')
outwriter = unicodecsv.writer(outfile)

with open(datafile, 'rb') as csvfile:
    linereader = csv.reader(csvfile, delimiter=',')
    count=0
    for row in linereader:
        
        url = row[7]
        count = count+1
        if (url and count > 1):
            try:
                html = urllib.urlopen(url).read()
            except Exception, e:
                        print "Error getting url" % url
            fburl = re.search("www\.facebook\.com\/(.*?)\"", html)
            if fburl:
                fbpages=fburl.groups()
            else:
                fbpages=''
            outwriter.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],fbpages])
