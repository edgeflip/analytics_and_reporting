#/usr/bin/python
from generate_data_for_export import create_events_file
from generate_report import generate_daily_report
import time
from virginia_send_to_s3 import send_to_s3
from time import strftime
from generate_report import email_report

if __name__ == '__main__':
	try:
		timestamp = open('timestamp.txt','r').read()
	except IOError:
		timestamp = str(int(time.time()))
	generate_daily_report(3,timestamp)
	create_events_file(3)
	send_to_s3()
	email_report(3)
