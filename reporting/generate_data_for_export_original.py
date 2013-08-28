#!/usr/bin/env python
from navigate_db import PySql        
import MySQLdb as mysql
import time
import csv
import os
import json
import urllib2


f = open('dbcreds.txt','r')
d = f.read().split('\n')
f.close()
tool = PySql(d[0], d[1], d[2], d[3])
tool.connect()


#tool = PySql('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
#tool.connect()

auth_with_timestamp = "SELECT session_id,updated,fbid,type,CASE type WHEN 'authorized' THEN 1 WHEN 'auth_fail' THEN 0 END FROM events WHERE (updated > FROM_UNIXTIME(%s) AND campaign_id='3') AND (type='authorized' or type='auth_fail');"
auth_without_timestamp = "SELECT session_id,updated,fbid,type,CASE type WHEN 'authorized' THEN 1 WHEN 'auth_fail' THEN 0 END FROM events WHERE campaign_id='3' AND (type='authorized' OR type='auth_fail');"


share_with_timestamp = "SELECT session_id,updated,fbid,friend_fbid,type,CASE type WHEN 'shared' THEN 1 WHEN 'shown' THEN 0 WHEN 'suppressed' THEN 0 WHEN 'share_fail' THEN 0 WHEN 'share_click' THEN 0 END FROM events WHERE (campaign_id='3' AND updated > FROM_UNIXTIME(%s)) AND (type='shared' OR type='shown' OR type='suppressed' OR type='share_fail' OR type='share_click');"
share_without_timestamp = "SELECT session_id,updated,fbid,friend_fbid,type,CASE type WHEN 'shared' THEN 1 WHEN 'shown' THEN 0 WHEN 'suppressed' THEN 0 WHEN 'share_fail' THEN 0 WHEN 'share_click' THEN 0 END FROM events WHERE campaign_id='3' AND (type='shared' OR type='shown' OR type='suppressed' OR type='share_fail' OR type='share_click');"


clickback_with_timestamp = "SELECT session_id, updated, fbid, CASE type WHEN 'clickback' THEN 1 END FROM events WHERE (type='clickback' AND updated > FROM_UNIXTIME(%s)) AND campaign_id='3';"
clickback_without_timestamp = "SELECT session_id, updated, fbid, CASE type WHEN 'clickback' THEN 1 END FROM events WHERE type='clickback' AND campaign_id='3';"



def create_auth_file_v2(timestamp=None):
	if timestamp:
		_all = tool.query(auth_with_timestamp.format(timestamp))
	else:
		_all = tool.query(auth_without_timestamp)

	f = open("auth_file.csv", "wb")
	csvfile = csv.writer(f, delimiter=',')
	csvfile.writerows(_all)
	f.close()
	print "Auth file written"

def create_share_file_v2(timestamp=None):
	if timestamp:
		_all = tool.query(share_with_timestamp.format(timestamp))
	else:
		_all = tool.query(share_without_timestamp)
	f = open("share_file.csv", "wb")
	csvfile = csv.writer(f, delimiter=',')
	csvfile.writerows(_all)
	f.close()
	print "Share file written"

def create_clickback_file_v2(timestamp=None):
	if timestamp:
		_all = tool.query(clickback_with_timestamp.format(timestamp))
	else:
		_all = tool.query(clickback_without_timestamp)
	with open("clickbacks.csv", "wb") as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
	csvfile.writerows(_all)
	f.close()
	print "Clickbacks file written"




####################################################################################################################################################

# OLD ALGORITHMS


def create_auth_file(timestamp=None):
	if timestamp:
		_all = tool.query("select session_id,fbid,type,updated from events where (updated > FROM_UNIXTIME(%s) AND campaign_id='3') AND (type='authorized' or type='auth_fail')" % timestamp)
	else:
		_all = tool.query("select session_id,fbid,type,updated from events where (type='authorized' AND campaign_id='3') OR type='auth_fail'")
	
	try:
		os.remove('authorization_data.csv')
	except OSError:
		pass

	try:
		from ids import ids
		os.remove("ids.py")
	except ImportError:
		ids = {"data": []}
	f = open("ids.py", "w")

	with open('authorization_data.csv', 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for each in _all:
			if each[0] == None:
				session_id = 'null'
			else:
                		session_id = each[0]
			if each[1] == None:
				fbid = 'null'
			else:
                        	fbid = each[1]
			if each[2] == None:
				_type = 'null'
			else:
                       		_type = each[2]
                        updated = str(each[3])
                        if _type == 'authorized':
                                writer.writerow([session_id,updated,fbid,str(1)])
                        elif _type == 'auth_fail':
                                writer.writerow([session_id,updated,fbid,str(0)])

			# add our current fbid to the file of facebook ids
			if fbid not in ids["data"] and str(fbid).isdigit():
				ids["data"].append(fbid)
			else:
				pass

		jsoned = json.dumps(ids)
		f.write("ids = %s" % jsoned)
		f.close()
		print "Facebook id file updated updated"

	print "Authorization data written\n+++++++++++\n"




	
# this is the algorithm we will want to use for shares

def create_share_file(timestamp=None):
	if timestamp:
		_all = tool.query("select session_id,fbid,friend_fbid,type,updated from events where (updated > FROM_UNIXTIME(%s) AND campaign_id='3') AND (type='shown' or type='shared' or type='share_click' or type='share_fail' or type='suppressed')" % timestamp)
	else:	
		_all = tool.query("select session_id,fbid,friend_fbid,type,updated from events where campaign_id='3' AND (type='shown' or type='shared' or type='share_fail' or type='suppressed')")
	
	try:
		os.remove('share_data.csv')
	except OSError:
		pass

	try:
		from ids import ids
		os.remove("ids.py")
	except ImportError:
		ids = {"data": []}

	f = open("ids.py","w")

	with open('share_data.csv', 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for each in _all:
			session_id = each[0]
			fbid = each[1]
			friend_fbid = each[2]
			_type = each[3]
			updated = each[4]
			if _type == 'shared':
				to_write = [session_id,updated,fbid,friend_fbid,str(1)]
			else:
				to_write = [session_id,updated,fbid,friend_fbid,str(0)]
			writer.writerow(to_write)

			if fbid not in ids["data"] and str(fbid).isdigit():
				ids["data"].append(fbid)
			else:
				pass
		jsoned = json.dumps(ids)
		f.write("ids = %s" % jsoned)
		f.close()
		print "Facebook id file updated"		
				
	print "Shared data written to file\n++++++++++\n"


"""
	create_share_file2 handles duplicate entries in the data and just answers the question of
	whether a primary shared with a secondary, not how many times that primary had the opportunity
	to share with the secondary.  it is simply a boolean data set of shared or not between primary
	and secondary.  this might be useful elsewhere, but I inadvertanly took this more difficult
	approach to solving the problem it was being written for.  we don't need all the functionality
	that this algorithm achieves right now.
"""

def create_share_file2():
	_all = tool.query("select session_id,fbid,friend_fbid,type,updated from events where type='shown' or type='shared' or type='share_fail' or type='suppressed'")
	new = []
	for each in _all:
		session_id = each[0]
    		fbid = each[1]
    		friend_fbid = each[2]
    		_type = each[3]
    		updated = str(each[4])
    		if len([e for e in new if e[2] == fbid and e[3] == friend_fbid]) == 0:
        		this_grouping_others = [j for j in _all if j[1] == fbid and j[2] == friend_fbid]
        		test = [i for i in this_grouping_others if i[3] == 'shared']
        		if len(test) > 0:
            			new_val = test[0]
            			new_to_add = [new_val[0], new_val[4], new_val[1], new_val[2], str(1)]
            			new.append(new_to_add)
        		else:
            			new_to_add = [session_id, updated, fbid, friend_fbid, str(0)]
            			new.append(new_to_add)
    		else:
        		pass
	with open('share_data.csv', 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for i in new:
			writer.writerow(i)
	print "Shared data written"




def create_clickback_file(timestamp=None):
	if timestamp:
		_all = tool.query("select session_id,fbid,updated from events where (updated > FROM_UNIXTIME(%s) AND type='clickback') AND campaign_id='3'" % timestamp)
	else:
		_all = tool.query("select session_id,fbid,updated from events where type='clickback' AND campaign_id='3'")

	try:
		os.remove('clickback_data.csv')
	except OSError:
		pass

	try:
		from ids import ids
		os.remove("ids.py")
	except ImportError:
		ids = {"data": []}
	
	f = open("ids.py", "w")

	with open('clickback_data.csv', 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for each in _all:
			session = each[0]
			if each[1] == None:
				fbid = 'none'
			else:
				fbid = each[1]
			updated = str(each[2])
			struct = [session, updated, fbid, str(1)]
			writer.writerow(struct)

			if fbid.isdigit() and fbid not in ids["data"]:
				ids["data"].append(fbid)
			else:
				pass
		jsoned = json.dumps(ids)
		f.write("ids = %s" % jsoned)
		f.close()
		print "Facebook id file updated"
	print "Clickback data written\n++++++++++\n"

	

def make_fbid_reference(ids):

	try:
		f = open('reference_table.csv','r')
		reference_table = f.read()
		f.read()
		# we need this to check against what we have in our token_data file as well
		# as to reconstruct our reference_table.csv file
		refs = reference_table.split('\n')
		refs.pop()
		refs = [i.split(',') for i in refs]	
		current_users = [i[0] for i in refs if len(refs) > 1]
		
		users_to_add = [i for i in ids["data"] if i not in current_users]
		with open('reference_table.csv', 'ab') as csvfile:
			csvfile.seek(0,os.SEEK_END)
			writer = csv.writer(csvfile,delimiter=',')

			for _id in users_to_add:
				res = tool.query("select fname,lname,city,state,birthday from users where fbid='%s'" % _id)
				fname = res[0][0]
				lname = res[0][1]
				city = res[0][2]
				state = res[0][3]
				bday = res[0][4]
			
				row = [_id,fname,lname,city,state,bday]

				writer.writerow(row)

			print "Reference table updated"

	except IOError:
		with open('reference_table.csv','w') as csvfile:
			writer = csv.writer(csvfile,delimiter=',')
			
			for fbid in ids["data"]:
				res = tool.query("select fname,lname,city,state,birthday from users where fbid='%s'" % fbid)
				
				fname = res[0][0]
				lname = res[0][1]
				city = res[0][2]
				state = res[0][3]
				bday = res[0][4]
				row = [fbid,fname,lname,city,state,bday]

				writer.writerow(row)
				
			print "Reference table written\n\n END\n\n"


