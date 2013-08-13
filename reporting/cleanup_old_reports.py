#!/usr/bin/env python
"""
    A simple program to be executed just after our daemonized
    report generation programs execute in order to clean up our
    directory of old reports
"""
import os
import time
import datetime

if __name__ == '__main__':
    dir_files = os.listdir('/home/ubuntu/')
    two_days = time.time() - 172800
    map(lambda x: x, 
	[
	 map(lambda x: os.remove('/home/ubuntu/%s' % x) if x.startswith('events_') and time.mktime(datetime.datetime(int(x.split('_')[4].split('.')[0]), int(x.split('_')[2]), int(x.split('_')[3])).timetuple()) < two_days else None, dir_files), 
	 map(lambda x: os.remove('/home/ubuntu/%s' % x) if x.startswith('ref_table') and time.mktime(datetime.datetime(int(x.split('_')[4].split('.')[0]), int(x.split('_')[2]), int(x.split('_')[3])).timetuple()) < two_days else None, dir_files), 
	 map(lambda x: os.remove('/home/ubuntu/%s' % x) if x.startswith('report_') and x.endswith('.csv') and time.mktime(datetime.datetime(int(x.split('_')[3].split('.')[0]), int(x.split('_')[1]), int(x.split('_')[2])).timetuple()) < two_days else None, dir_files)
	]
       )
    print "Finished"
