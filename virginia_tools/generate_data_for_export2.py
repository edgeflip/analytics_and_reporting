#!/urs/bin/env python
import MySQLdb as mysql
from navigate_db import PySql
import json
import os
import time
import csv
from time import strftime


conn = mysql.connect('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
cur = conn.cursor()
tool = PySql(cur)



def create_events_table(timestamp=None):
	try:
		os.remove('timestamp.txt')
	except OSError:
		pass

	try:
		from all_campaign_users import all_campaign_users
		os.remove("all_campaign_users.py")
	except ImportError:
		all_campaign_users = {"data": []}
	
	if timestamp != None:
		_all = tool.query("select session_id, updated as action_time, fbid, friend_fbid, type, activity_id from events where campaign_id='3' and updated > FROM_UNIXTIME('%s')" % timestamp)
		timestamp = str(int(time.time()))
		f = open('timestamp.txt','w')
		f.write(timestamp)
		f.close()
	else:
		_all = tool.query("select session_id, updated as action_time, fbid, friend_fbid, type, activity_id from events where campaign_id='3'")			
		timestamp = str(int(time.time()))
		f = open('timestamp.txt','w')
		f.write(timestamp)
		f.close()	
	
	#with open("events_file_%s_%s.csv" % (month,day),"wb") as csvfile:
	with open("events_file_%s.csv" % str(int(time.time())), "wb") as csvfile:
		# go to the end of our events_file to append our new data
		writer = csv.writer(csvfile, delimiter=',')

		for each in _all:
			session_id = each[0]
			action_time = str(each[1])
			if each[2] == None:
				fbid = 'none'
			else:
				fbid = each[2]
			if each[3] == None:
				friend_fbid = 'none'
			else:
				friend_fbid = each[3]
			_type = each[4]
			activity_id = each[5]
			row = [session_id, action_time, fbid, friend_fbid, _type, activity_id]
			writer.writerow(row)

			if fbid not in all_campaign_users["data"] and fbid != None:
				all_campaign_users["data"].append(fbid)
			else:
				pass
			if friend_fbid not in all_campaign_users["data"] and friend_fbid != None:
				all_campaign_users["data"].append(friend_fbid)


	jsoned = json.dumps(all_campaign_users)
	f = open("all_campaign_users.py", "w")
	f.write("all_campaign_users = %s" % jsoned)
	f.close()
	print "All campaign users file updated\n"	
					
	print "Events table generated\n\n"


def create_reference_table(all_campaign_users):
	try:
		f1 = open('ref_table.csv','r').read()
		f1 = f1.split('\n')
		f1 = [i.split(',') for i in f1]
		already_referenced = [j[0] for j in f1]
	except IOError:
		already_referenced = []

	f = open('ref_table.csv','ab')
	f.seek(0, os.SEEK_END)
	writer = csv.writer(f, delimiter=',')
	
	for int_fbid in all_campaign_users["data"]:
		fbid = str(int_fbid)
		if fbid not in already_referenced:
			query = tool.query("select fname,lname,city,state,birthday from users where fbid='%s'" % fbid)

			try:
				fname = query[0][0]
			except IndexError:
				fname = 'n/a'
			try:
				lname = query[0][1]
			except IndexError:
				lname = 'n/a'
			try:
				city = query[0][2]
			except IndexError:
				city = 'n/a'
			try:
				state = query[0][3]
			except IndexError:
				state = 'n/a'
			try:
				birthday = str(query[0][4])
			except IndexError:
				birthday = 'n/a'

			row = [fbid, fname, lname, city, state, birthday]		
			writer.writerow(row)
	
		else:
			pass

	f.close()
	
	print "Reference file updated\n"	




