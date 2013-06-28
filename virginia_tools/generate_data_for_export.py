#!/usr/bin/env python
from navigate_db import PySql        
import MySQLdb as mysql
import time
import csv
import os


conn = mysql.connect('edgeflip-db.efstaging.com', 'root', '9uDTlOqFmTURJcb', 'edgeflip')
cur = conn.cursor()
tool = PySql(cur)

def create_auth_file(timestamp=None):
	if timestamp:
		_all = tool.query("select session_id,fbid,type,updated from events where updated > FROM_UNIXTIME(%s) AND (type='authorized' or type='auth_fail')" % timestamp)
	else:
		_all = tool.query("select session_id,fbid,type,updated from events where type='authorized' or type='auth_fail'")
	
	try:
		os.remove('authorization_data.csv')
	except OSError:
		pass

	with open('authorization_data.csv', 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for each in _all:
                	session_id = each[0]
                        fbid = each[1]
                        _type = each[2]
                        updated = str(each[3])
                        if _type == 'authorized':
                                writer.writerow([session_id,updated,fbid,str(1)])
                        elif _type == 'auth_fail':
                                writer.writerow([session_id,updated,fbid,str(0)])

	print "Authorization data written"




	
# this is the algorithm we will want to use for shares

def create_share_file(timestamp=None):
	if timestamp:
		_all = tool.query("select session_id,fbid,friend_fbid,type,updated from events where updated > FROM_UNIXTIME(%s) AND (type='shown' or type='shared' or type='share_click' or type='share_fail' or type='suppressed')" % timestamp)
	else:	
		_all = tool.query("select session_id,fbid,friend_fbid,type,updated from events where type='shown' or type='shared' or type='share_fail' or type='suppressed'")
	
	try:
		os.remove('share_data.csv')
	except OSError:
		pass

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
	print "Shared data written to file"


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
		_all = tool.query("select session_id,fbid,updated from events where updated > FROM_UNIXTIME(%s) AND type='clickback'" % timestamp)
	else:
		_all = tool.query("select session_id,fbid,updated from events where type='clickback'")

	try:
		os.remove('clickback_data.csv')
	except OSError:
		pass

	with open('clickback_data.csv', 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		for each in _all:
			struct = [each[0], each[2], each[1], str(1)]
			writer.writerow(struct)
	print "Clickback data written"
