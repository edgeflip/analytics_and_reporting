#!/usr/bin/env python
from generate_data_for_export import create_auth_file, create_share_file, create_clickback_file
from generate_data_for_export import Reference
from navigate_db import PySql
import MySQLdb as mysql
import time
import csv
import os

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
		from token_data import token_data
		make_reference(token_data)

	except ImportError:
		pass

	f = open('timestamp.txt', 'w')
	_time = str(int(time.time()))
	f.write(_time)
	f.close()

	print "Files written and timestamp updated"


if __name__ == '__main__':
	main()
	
