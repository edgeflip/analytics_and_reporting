#/usr/bin/python
from generate_data_for_export import create_events_file
from virginia_send_to_s3 import send_to_s3
from time import strftime
# mainly generate_master_report, generate_report2 and all it's dependencies (encodeDES, secret, etc...)
from generate_report import *
from cleanup import cleanup
import time, datetime

if __name__ == '__main__':
	try:
		timestamp = open('timestamp.txt','r').read()
	except IOError:
		# make a timestamp from a datetime object
		y = strftime('%Y')
		m = strftime('%m')
		d = str(int(strftime('%d'))-1)
		if len(d) == 1:
			d = '0'+d
		morning = datetime.datetime(int(y),int(m),int(d),00,00,00)
		timestamp = str(int(time.mktime(morning.timetuple())))
	# make a new timestamp before the processes start so as to not lose data
	new_timestamp = str(int(time.time()))
	create_events_file(2,timestamp)
	send_to_s3()
	#email_report(3)
	#cleanup()
	#generate_report2(2,timestamp)
	#generate_master_report(timestamp)	
	f = open('timestamp.txt','w')
	f.write(new_timestamp)
	f.close()
	#_mail_master()
	
