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
	pertinent_info = orm.query('SELECT fbid,ownerid,token FROM tokens')
	#pertinent_info = orm.query('SELECT fbid,ownerid,token FROM tokens limit 1000')
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


# with a separate data object containing the JSON blobs relating to a user's most recent
# updates we can iterate through and crawl those user's walls to update our s3 with their info
def run_crawler():
	from boto.s3.connection import S3Connection
	from boto.s3.key import Key
	con = mysql.connect('edgeflip-db.efstaging.com', 'root', '9uDTlOqFmTURJcb', 'edgeflip')
	cur = con.cursor()
	orm = PySql(cur)
	all_info = orm.query('SELECT fbid,appid,ownerid,token FROM tokens')
	conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
	main_bucket = conn.get_bucket('fbcrawl1')
	subscriptions_bucket = conn.get_bucket('fbrealtime')
	for each in all_info:
		fbid = each[0]
		appid = each[1]
		ownerid = each[2]
		token = each[3]
		if not check_if_crawled(fbid,ownerid):
			key = '{0},{1}'.format(fbid,ownerid,appid)
			k = Key(main_bucket)
			k.key = key
			# use crawl_feed from below and set the key
			response = crawl_feed(fbid,ownerid,token)
			k.set_contents_from_string(response)
			# subscribe the user
			subscribe_user(fbid,appid,ownerid)
		else:
			key = '{0},{1}'.format(fbid,ownerid,appid)
			k = main_bucket.get_key(key)

			#############################################################################3
			# wont work
			sub_k = subscription_bucket.get_key(key)

			current_feed_data = json.loads(k.get_contents_as_string())
			current_realtime_data = json.loads(sub_k.get_contents_as_string())
			# delete data from subscription_bucket so we don't get it again on the 
			# next iteration
			#subscription_bucket.delete_key(key)
			
			try:
				# read the current_realtime_data most recent post time
				#current_realtime_data['somekey']['time']
				#recent_time = current_feed_data['feed']['data'][0]['updated_time']
			except KeyError:
				# take data from a month ago, 2419200 seconds = 28 days
				recent_time = time.time() - 2419200
			response = json.loads(crawl_feed_since(fbid,recent_time,access_token))
			# integrate the new data with our previously stored data
			current_feed_data.update(response)
			current_feed_data_string = json.dumps(current_feed_data)
			k.set_contents_from_string(current_feed_data_string)
	print "Done"


def always_crawl():
    # connect to s3 database
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    main_bucket = conn.get_bucket('fbcrawl1')
    token_bucket = conn.get_bucket('fbtokens')
    realtime_bucket = conn.get_bucket('fbrealtime')

    # edgeflip databse
    con = mysql.connect('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
    cur = con.cursor()
    orm = PySql(cur)
    most_data = orm.query('select fbid,appid,ownerid,token from tokens')

    new_count = 0
    update_count = 0
    for item in most_data:
        fbid = item[0]
        appid = item[1]
        ownerid = item[2]
        token = item[3]
        main_key = fbid+','+ownerid
        if not main_bucket.lookup(main_key):
            # crawl_feed returns a 
            response = crawl_feed(fbid,token)
            k = main_bucket.new_key()
            k.key = main_key
            k.set_contents_from_string(response)
            # put the fbid,ownerid, and token in token_bucket
            token_key = token_bucket.new_key()
            token_key.key = fbid
            token_key_struct = {fbid: [{owerid: token}]}
	    jsoned = json.dumps(token_key_struct)
            token_key.set_contents_from_string(jsoned)
            new_count += 1
            # otherwise we've already crawled our user and there should be information about
            # him/her in our main_bucket and our token_bucket
        else:
        # get everything from the subscribed updates
            if realtime_bucket.lookup(fbid):
                cur = realtime_bucket.get_key(fbid)
                data = json.loads(cur.get_contents_as_string())
                update_time = data['time']
                tokens = get_tokens_for_user(fbid, token_bucket)
                for ownerid, cur_token in tokens:
                    api = 'https://graph.facebook.com/{0}?fields=feed.since({1})&access_token={2}'
                    this_api = api.format(fbid,update_time,cur_token)
                    this_resposne = json.loads(urllib2.urlopen(this_api).read())
                    # data already stored related to user
                    pertaining_key = main_bucket.get_key(fbid+','+ownerid)
                    pertaining_data = json.loads(pertaining_key.get_contents_as_string())
                    # update the already stored data with the newly acquired data
                    # remember to convert to a json string to store in our s3 bucket
                    new_data = json.dumps(pertaining_data.update(this_response))
                    pertaining_key.set_contents_from_string(new_data)
                    udpate_count += 1
                    # if our current fbid isn't in the RealTime updates bucket, move along
            else:
                pass
    print "%s new users added and %s users" % (str(new_count), str(update_count))


			 

# we have an s3 bucket specifically for tokens so that when we've received an update from facebook
# about a user we can a) call this function to make sure we've got the token added and then
# b) call another function to use these tokens 

# {"fbid": [{"owner1": "token1"}, {"owner2": "token2"}, {"owner3": "token3"}]}

def add_tokens_to_bucket_then_return(fbid,friend_fbid,token,bucket):
	key = Key(bucket)
	k = key.get_key(fbid)
	cur_keys = json.loads(k.get_content_as_string())
	if len([i for i in cur_keys[fbid] if i.keys()[0] == friend_fbid]) == 0:
		return
	else:
		data = {friend_fbid:token}
		cur_keys[fbid].append(data)
	new_key_data = json.dumps(cur_keys)
	k.set_content_from_string(new_key_data)
	# return the data structure
	return cur_keys

# takes a fbid and a connection and returns a list of tuples (ownerid,token) to parse
# tokens = [('ownerid', 'token'), ('ownerid2', 'token2'), ('ownerid3', 'token3')....]
def get_tokens_for_user(fbid, conn)
	bucket = conn.get_bucket('fbtokens')
	k = Key(bucket)
	fbid_tokens_data = k.get_key(fbid)
	data = json.loads(fbid_tokens_data.get_contents_as_string())
	tokens = [i.items()[0] for i in data[fbid]]
	return tokens


# relies on get_tokens_for_user list and also the RealTime updates data because these functions
# will be called immediately after an update is received upon the next crawl
def crawl_all_tokens(fbid, tokens, conn, update_time):
	crawl_data_bucket = conn.get_bucket('fbcrawl1')
	api = 'https://graph.facebook.com/{0}?fields=feed.since(%s)&access_token={1}' % update_time
	for owernid,token in tokens:
		cur_key = crawl_data_bucket.get_key(fbid + ',' + ownerid)
		cur_key_data = json.loads(cur_key.get_contents_as_string())
		formatted = api.format(fbid,token)
		result = json.loads(urllib2.urlopen(formatted).read())
			
		

	
def crawl_feed(fbid,friend_fbid,access_token):
	api = 'https://graph.facebook.com/{0}?fields=feed&access_token={1}'
	formatted = api.format(fbid,access_token)
	response = urllib2.urlopen(formatted).read()
	json_response = json.dumps(response)
	return json_response

def crawl_feed_since(fbid,since,access_token):
	api = 'https://graph.facebook.com/{0}?fields=feed.since({1})&access_token={2}'
	formatted = api.format(fbid,str(since),access_token)
	response = urllib2.urlopen(formatted).read()
	json_response = json.dumps(response)
	return json_response


def check_if_crawled(fbid,friend_fbid):
	from boto.s3.connection import S3Connection
	from boto.s3.key import Key
	conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
	bucket = conn.get_bucket('fbcrawl1')
	if bucket.lookup('{0},{1}'.format(fbid,friend_fbid)):
		return True
	return False

# check s3 for a key that is in the form of 'fbid,friend_fbid' and if there is one use the update
# from facebook to run a query for the given user's feed since the "time" provided
# to_query = https://graph.facebook.com/fbid?fields=feed.since(response["time"])&access_token=d76lk766....
# urllib.urlopen(to_query).read()

def subscribe_user(fbid,appid,ownerid):
	callback_url = 'http://fbrealtime.s3-website-us-east-1.amazonaws.com/'
	api = 'https://graph.facebook.com/{0}/subscriptions?object=feed&callback_url={1}'
	formatted_subscribe = api.format(appid,callback_url)
	pass 



