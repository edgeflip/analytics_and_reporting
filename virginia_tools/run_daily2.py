#!/usr/bin/env python
from generate_data_for_export2 import create_events_table
from generate_data_for_export2 import create_reference_table
import os

def main():
	try:
		_timestamp = open('timestamp.txt','r')
		timestamp = _timestamp.read()

		create_events_table(timestamp)

		os.remove('timestamp.txt')
		
	except IOError:
		create_events_table()
	
	from all_campaign_users import all_campaign_users
	create_reference_table(all_campaign_users)


if __name__ == '__main__':
	main()


