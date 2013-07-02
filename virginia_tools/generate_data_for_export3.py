#!/usr/bin/env python
from navigate_db import PySql
import MySQLdb as mysql
import csv
import os
import time

conn = mysql.connect('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
cur = conn.cursor()
tool = PySql(cur)

def create_events_file(campaign_id):
	no_time = "select session_id, updated as action_time, fbid, friend_fbid, type, activity_id from events where campaign_id='{0}'" 
	_time = "select session_id, updated as action_time, fbid, friend_fbid, type, activity_id from events where campaign_id='{0}' and updated > FROM_UNIXTIME({1})"

	users_no_time = "select fbid,fname,lname,city,state,birthday from users where fbid in (select fbid from events where campaign_id='{0}')"
	users_time = "select fbid,fname,lname,city,state,birthday from users where updated > from_unixtime({1}) and fbid in (select fbid from events where campaign_id='{0}' and updated > from_unixtime({2}))"
	try:
		timestamp = open('timestamp.txt','r').read()
		os.remove('timestamp.txt')
		query_formatted = _time.format(str(campaign_id),timestamp)
		events_res = tool.query(query_formatted)
		user_query = users_time.format(campaign_id,timestamp, timestamp)
		users_res = tool.query(user_query)
		
	except IOError:
		query_formatted = no_time.format(str(campaign_id))
		events_res = tool.query(query_formatted)
		user_query = users_no_time.format(campaign_id)
		users_res = tool.query(user_query)
	f = open('timestamp.txt','w')
	f.write(str(int(time.time())))
	f.close()
	events_writer = csv.writer(open('events_file.csv','wb'), delimiter=',')
	ref = open('ref_table.csv','ab')
	ref.seek(0,os.SEEK_END)
	ref_writer = csv.writer(ref,delimiter=',')
	events_writer.writerows(events_res)
	ref_writer.writerows(users_res)

	print "Data written"
