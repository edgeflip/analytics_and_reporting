#!/usr/bin/env python
from time import strftime
import os

def cleanup():
	m = strftime('%m')
	d = strftime('%d')
	y = strftime('%Y')

	event = 'events_file_{0}_{1}_{2}.csv'
	ref = 'ref_table_{0}_{1}_{2}.csv'
	report = 'report_{0}_{1}_{2}.txt'
	
	try:
		if int(d) == 1:
			
			m = '0' + str(int(m)-1)
			if int(m) == 1 or 3 or 5 or 7 or 8 or 10 or 12:
				d = '31'
			else:
				d = '30'

		else:
			if len(str(int(d))) == 1:
				d = '0' + str(int(d)-1)
			else:
				d = str(int(d)-1)

		event_formatted = event.format(m,d,y)
		ref_formatted = ref.format(m,d,y)
		report_formatted = report.format(m,d,y)
		os.remove(event_formatted)
		os.remove(ref_formatted)
		os.remove(report_formatted)
	
	except OSError:
		pass


if __name__ == '__main__':
	cleanup()
