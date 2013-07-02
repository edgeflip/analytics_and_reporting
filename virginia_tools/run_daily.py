#!/usr/bin/env python
from generate_data_for_export import create_auth_file, create_share_file, create_clickback_file
from generate_data_for_export import make_fbid_reference
from navigate_db import PySql
import MySQLdb as mysql
import time
import csv
import os
import json
import urllib2
from generate_report import generate_basic

# with PySql we can just pass a cursor object to instantiate it



def main():	
	try:
		time_file = open('timestamp.txt','r')
		timestamp = time_file.read()
		os.remove('timestamp.txt')
		
		create_auth_file(timestamp)
		create_share_file(timestamp)
		create_clickback_file(timestamp)

	except IOError:
		create_auth_file()
		create_share_file()
		create_clickback_file()


	try:
		from ids import ids
		make_fbid_reference(ids)

	except ImportError:
		pass

	try:
		os.remove("timestamp.txt")
	except OSError:
		pass
	f = open('timestamp.txt', 'w')
	_time = str(int(time.time()))
	f.write(_time)
	f.close()

	# generate report
	generate_basic()

	print "Files written and timestamp updated"


if __name__ == '__main__':
	main()
	
