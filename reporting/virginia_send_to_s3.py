#!/usr/bin/env python
from boto.s3.connection import S3Connection
from time import strftime
from con_s3 import connect_s3

# invoke this daily after the generate_data_for_export3 methods have done their work to send the files they
# generate to our s3 bucket for access by virginia
def send_to_s3():
	conn = connect_s3()
	bucket = conn.get_bucket('virginia_bucket')
	m = strftime('%m')
	d = strftime('%d')
	y = strftime('%Y')
	events_file = 'events_file_{0}_{1}_{2}'.format(m,d,y)
	ref_file = 'ref_table_{0}_{1}_{2}'.format(m,d,y)

	event_key = bucket.new_key()
	event_key.key = events_file
	event_key.set_contents_from_filename(events_file + '.csv')
	event_key.set_acl('public-read')

	ref_key = bucket.new_key()
	ref_key.key = ref_file
	ref_key.set_contents_from_filename(ref_file + '.csv')
	ref_key.set_acl('public-read')

	print "Data added to s3 bucket"
