#!/usr/bin/env python
import mechanize
import cookielib
import json
import MySQLdb as mysql
from navigate_db import PySql
import urllib2
from time import strftime
import time


# not necessarily needed, but good for testing stuff with my own account and credentials
class GoodBrowser(object):
	def __init__(self):
		self.br = mechanize.Browser()
		self.cj = cookielib.LWPCookieJar()
	# this method allows us to look like a web browser
	def add_credentials(self):
		self.br.set_debug_http(True)
		self.br.set_debug_redirects(True)
		self.br.set_handle_robots(False)
		self.br.set_handle_equiv(True)
		self.br.set_handle_gzip(True)
		self.br.set_cookiejar(self.cj)
		self.br.addheaders = [('User-Agent', 'Mozilla/5.0(X11;U;Linux i686;en-US;rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

	def go(self,url):
		self.br.open(url)

	def facebook_login(self):
		self.go('http://www.facebook.com')
		self.br.select_form(nr=0)
		email = raw_input('facebook email: ')
		password = raw_input('facebook password: ')		
		self.br.form['email'] = email
		self.br.form['pass'] = password
		self.br.submit()
		print "\n+++++ Logged into facebook as %s +++++\n" % email

	# need to have visited the graph api explorer and generated a token
	def graph_query(self, fbid, fields, access_token):
		graph_api = 'https://graph.facebook.com/{0}?fields={1}&access_token={2}'

		formatted = graph_api.format(fbid,fields,access_token)
		# make query
		self.go(formatted)
		json_response = json.loads(self.br.response().read())
		return json_response


# algorithm to crawl everyones feeds and compile some data objects relating to
# those feeds
# an object with the most recent post's "updated_time" is stored and written
# to a file so that we can pull those user-specific updated_time's to use in our
# subsequent queries


def crawl_all_feeds():
	start = time.time()
	try:
		from times import times
	except ImportError:
		pass
	data_object = {"data": []}
	time_object = {}
	api = 'https://graph.facebook.com/{0}?fields={1}&access_token={2}'
	feed = 'feed'
	con = mysql.connect('edgeflip-db.efstaging.com', 'root', '9uDTlOqFmTURJcb', 'edgeflip')
	cur = con.cursor()
	orm = PySql(cur)
	#pertinent_info = orm.query('SELECT fbid,ownerid,token FROM tokens')
	pertinent_info = orm.query('SELECT fbid,ownerid,token FROM tokens limit 1000')
	# descend into the iterable returned by our PySql instance
	for i in range(len(pertinent_info)):
		# key = pertinent_info[i][0] is the user's feed we are crawling
		key = str(pertinent_info[i][0])
		primary = str(pertinent_info[i][1])
		access_token = pertinent_info[i][2]
		cur_data = {key: {}}
		cur_data[key]["primary_fbid"] = primary
	
		# if we have a times file, use the current key and primary to query the most recent
		# post's updated_time that we have and use that as our threshold of where to start
		# getting data from his/her wall
		try:	
			pertinent_time = times[key][primary]
			feed = feed + '.since(%s)' % pertinent_time
			formatted_query = api.format(key,feed,access_token)
		except NameError:
			formatted_query = api.format(key,feed,access_token)
		try:
			data = json.loads(urllib2.urlopen(formatted_query).read())
		except urllib2.HTTPError:
			time.sleep(601)
		# to store in our time_object as a reference for the most recently posted item
		# pertaning to the specific key's feed
		try:
			most_recent_post = data["feed"]["data"][0]["updated_time"]
		except KeyError:
			most_recent_post = strftime('%Y-%m-%d %H:%M:%S')
		to_update = {key: {primary: most_recent_post}}
		time_object.update(to_update)
		data_object["data"].append(data)
	# if we have a times.py file, delete it, if not, move on
	try:
		os.remove('times.py')
	except NameError:
		pass
	times_str = json.dumps(time_object)
	time_file = open('times.py','w')
	time_file.write('times = %s' % times_str)
	time_file.close()
	end = time.time()
	total = end-start
	print "%s seconds to compute" % str(total)	
	return data_object
