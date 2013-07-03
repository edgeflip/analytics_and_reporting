#!/usr/bin/env python
from generate_data_for_export import create_auth_file_v2
from generate_data_for_export import create_share_file_v2
from generate_data_for_export import create_clickback_file_v2
import os, time

def main():
	try:
		_time = open('timestamp.txt','r').read()
		os.remove('timestamp.txt')
		
		create_auth_file_v2(_time)
		create_share_file_v2(_time)
		create_clickback_file_v2(_time)
	
	except IOError:
		create_auth_file_v2()
		create_share_file_v2()
		create_clickback_file_v2()

	f = open('timestamp.txt','w')
	f.write(str(int(time.time())))
	f.close()
	print "Done"


if __name__ == '__main__':
	main()
