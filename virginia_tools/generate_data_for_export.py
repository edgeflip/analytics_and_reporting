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

	# try opening our fbid and token file
	# {"fbid": [tokens,...], "fbid": [tokens...], "fbid": [tokens...]}

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

			try:
				from token_data import token_data
				if fbid not in token_data.keys():
					tokens = tool.query("select token from tokens where fbid='%s'" % fbid)
					token_data[fbid] = [i[0] for i in tokens]
					os.remove('token_data.py')
					f = open('token_data.py','w')
					f.write("token_data = %s" json.dumps(token_data))
					f.close()
					print "Token data file updated\n"
				else:
					pass
			except ImportError:
				pass

	print "Authorization data written\n\n"




	
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

			try:
				from token_data import token_data
				#os.remove('token_data.py')
				if fbid not in token_data.keys():
					os.remove('token_data.py')
					_tokens = tool.query("select token from tokens where fbid='%s'" % fbid)
					tokens = [i[0] for i in _tokens]
					token_data[fbid] = tokens
					f = open('token_data.py', 'w')
					f.write('token_data = %s' % json.dumps(token_data))
					f.close()	
					print "Token data file updated\n"
				else:
					pass
			except ImportError:
				pass 
					
				
	print "Shared data written to file\n\n"


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

			try:
				from token_data import token_data
				if struct[2].isdigit() and struct[2] not in token_data.keys():
					tokens = tool.query("select token from tokens where fbid='%s'" % struct[2])
					token_data[struct[2]] = [i[0] for i in tokens]
				else:
					pass
			except ImportError:
				pass					

	print "Clickback data written"



def make_fbid_reference(token_data):
	attrs = ['fname','lname','city','state','dob']
	api = 'https://graph.facebook.com/{0}?fields=fbid,fname,lname,city,state,birthday&access_token={1}'
	try:
		f = open('reference_table.csv','r')
		reference_table = f.read()
		f.read()
		# we need this to check against what we have in our token_data file as well
		# as to reconstruct our reference_table.csv file
		refs = reference_table.split('\n')
		
		current_users = [i[0] for i in refs]
		
		token_data_users_not_added = [i for i in token_data.keys() if i not in current_users]
		with open('reference_table.csv', 'ab') as csvfile:
			csvfile.seek(0,os.SEEK_END)
			writer = csv.writer(csvfile,delimiter=',')

			for _id in token_data_users_not_added:
				formatted = api.format(_id, token_data[_id][0])
				res = urllib2.urlopen(formatted).read()
				fname = res["first_name"]
				lname = res["last_name"]
				loc = res["location"]["name"].split(',')
				city = loc[0]
				try:
					state = loc[1]
				except IndexError:
					state = 'N/A'
				bday = res["birthday"]
			
				row = [_id,fname,lname,city,state,bday]

				writer.writerow(row)

			print "Reference table updated"

	except IOError:
		with open('reference_table.csv','w') as csvfile:
			writer = csv.writer(csvfile,delimiter=',')
			
			for fbid in token_data.keys():
				attrs = ['first_name','last_name','location','birthday']
				token = token_data[fbid]
				formatted = api.format(fbid,token)
				_res = urllib2.urlopen(formatted)
				res = json.loads(_res.read())
				fname = res["first_name"]
				lname = res["last_name"]
				loc = res["location"]["name"].split(',')
				city = loc[0]
				try:
					state = loc[1]
				except IndexError:
					state = 'N/A'
				bday = res["birthday"]

				row = [fbid,fname,lname,city,state,bday]

				writer.writerow(row)
				
			print "Reference table written"
