#!/usr/bin/env python
from navigate_db import PySql
import MySQLdb as mysql
import csv
import os
import time
from time import strftime
from generate_data_for_export_original import tool

#tool = PySql('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
#tool.connect()


# updated to now take a client_id as paremeter
def create_events_file(client_id, timestamp=None):
	no_time = "select session_id, updated as action_time, fbid, friend_fbid, type, activity_id from events where campaign_id in (select campaign_id from campaigns where client_id='{0}') and content_id in (select content_id from client_content where client_id='{0}')" 
	_time = "select session_id, updated as action_time, fbid, friend_fbid, type, activity_id from events where campaign_id in (select campaign_id from campaigns where client_id='{0}') and content_id in (select content_id from client_content where client_id='{0}') and updated > FROM_UNIXTIME({1})"

	users_no_time = "select fbid,fname,lname,city,state,birthday from users where fbid in (select fbid from events where campaign_id in (select campaign_id from campaigns where client_id='{0}') and content_id in (select content_id from client_content where client_id='{0}') union select friend_fbid from events where campaign_id in (select campaign_id from campaigns where client_id='{0}') and content_id in (select content_id from client_content where client_id='{0}'))"
	users_time = "select fbid,fname,lname,city,state,birthday from users where updated > from_unixtime({1}) and fbid in (select fbid from events where campaign_id in (select campaign_id from campaigns where client_id='{0}') and content_id in (select content_id from client_content where client_id='{0}') and updated > from_unixtime({1}) union select friend_fbid from events where campaign_id in (select campaign_id from campaigns where client_id='{0}') and content_id in (select content_id from client_content where client_id='{0}'))"
	if timestamp:
		query_formatted = _time.format(str(client_id),timestamp)
		events_res = tool.query(query_formatted)
		user_query = users_time.format(str(client_id),timestamp)
		users_res = tool.query(user_query)
		
	else:
		query_formatted = no_time.format(str(client_id))
		events_res = tool.query(query_formatted)
		user_query = users_no_time.format(str(client_id))
		users_res = tool.query(user_query)
	#f = open('timestamp.txt','w')
	#f.write(str(int(time.time())))
	#f.close()
	m = strftime('%m')
	d = str(int(strftime('%d'))-1)
	if len(d) == 1:
		d = '0'+d
	y = strftime('%Y')
	events_writer = csv.writer(open('events_file_{0}_{1}_{2}.csv'.format(m,d,y),'wb'), delimiter=',')
	ref = open('ref_table_{0}_{1}_{2}.csv'.format(m,d,y),'wb')
	#ref.seek(0,os.SEEK_END)
	ref_writer = csv.writer(ref,delimiter=',')
	if len(events_res) == 0:
		events_writer.writerow(['No new data'])
	else:
		events_writer.writerow(['session id', 'action time', 'fbid', 'friend fbid', 'action type'])
		events_writer.writerows(events_res)
	if len(users_res) == 0:
		ref_writer.writerow(['No new users'])
	else:
		ref_writer.writerow(['fbid', 'first name', 'last name', 'city', 'state', 'birthday'])
		ref_writer.writerows(users_res)

	print "Data written"
