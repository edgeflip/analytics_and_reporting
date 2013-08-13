#!/usr/bin/env python
import cookielib
import json
import MySQLdb as mysql
from navigate_db import PySql
import urllib2
from time import strftime
import time
import csv
from boto.s3.connection import S3Connection
from crawl_metrics import metrics


"""
    using the metrics algorithm from crawl_metrics.py we will build metrics for our users in a paralell
    manner simultaneously to updated their feeds in the s3 bucket.  The process will be as follows:
    1) when always_crawl_from_database runs we get each user's feed and simultaenously pass that feed to metrics
       to build a metric object around, then we will store both objects (feed and metrics) in their respective
       locations
    2) when crawl_realtime_updates runs we get each pertinent user's updates and add them to the feed that we
       currently have. within the same code execution we will pass the NEW feed with updates to metrics and
       get back an updated metric for the pertinent user at which point we will then just replace any metric
       we had stored for him/her
"""

def build_metrics():
    pass


"""
    always_crawl_from_database takes a crawl_timestamp as input and crawls all those users and their respective
    tokens who have been added to the database since the timestamp. if there is no timestamp it means this is
    the first time the method is being called and we will crawl all users entire feed.  always_crawl_from_database
    also creates a file with a json string containing all ids that were crawled so we can cross check it when
    our next method, crawl_realtime_updates is called.  this file will be to eliminate repetitive crawling when
    facebook sends an update on a newly registered use that we haven't crawled yet and we crawl them with
    always_crawl_from_database and then go attempt to execute a crawl_realtime_updates crawl on them.  since we
    just crawled their entire wall we don't need to crawl their wall since the updated
"""


def always_crawl_from_database(tool,crawl_timestamp = None):
    import csv
    # connect to s3 database
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    main_bucket = conn.get_bucket('fbcrawl1')
    token_bucket = conn.get_bucket('fbtokens')
    realtime_bucket = conn.get_bucket('fbrealtime')
    metric_bucket = conn.get_bucket('metric_bucket')
    if not crawl_timestamp:
        most_data = tool.query('select fbid,ownerid,token from tokens')
    else:
        most_data = tool.query('select fbid,ownerid,token from tokens where updated > FROM_UNIXTIME(%s)' % crawl_timestamp)
    crawl_log = open('crawl_log.csv','wb')
    crawl_log_writer = csv.writer(crawl_log,delimiter=',')
    new_count = 0
    for item in most_data:
        fbid = str(item[0])
        ownerid = str(item[1])
        token = item[2]
        main_key = str(fbid)+','+str(ownerid)
        if not main_bucket.lookup(main_key):
            # go ahead and write the fbid to the csv log file
            crawl_log_writer.writerow([fbid])
            # crawl_feed returns a json blob of the users feed
            # on this pass of the code we are getting the entire feed
	    # this will sometimes yield a urllib2.URLError so when it does
	    # time.sleep() the program for just a couple of seconds and try again
	  
            try: 
                response = crawl_feed(fbid,token)
		return response, token, main_key
		break
		exit()
	    except (urllib2.URLError, urllib2.HTTPError):
		response = ''
	    #except (urllib2.URLError, urllib2.HTTPError):
	    #	response = ''
	    
	    k = main_bucket.new_key()
            # set the bucket's key to be fbid,ownerid
            k.key = main_key
          


	    # META DATA FOR POST IDS TO AVOID DUPLICATE POSTS IN THIS FEED
	    # we need to call json.loads() on response twice
	    try: 
	        response = json.loads(response)
                response = json.loads(response)
	
	        try:
		    post_ids = list(set([each['id'] for each in response['feed']['data'] if 'id' in each.keys()]))
	        except KeyError:
		    post_ids = []
	        # SET THE META DATA
		the_data = {"data": post_ids}
	        k.set_metadata('data', json.dumps(the_data))

                # METRICS PORTION
	    
                # remember that our response above was a json string and we need to pass the metrics algorithm an object
	        # there are times when this will return a TypeError or KeyError depending on what the response looks like
		# that is passed to the metrics algorithm
	        try:
	            metric_object = metrics(response)
                    m_key = metric_bucket.new_key()
                    # key will be the same
                    m_key.key = main_key
                    m_key.set_contents_from_string(json.dumps(metric_object))
		except (TypeError, KeyError):
		    pass

	    # if response=json.loads(response) above fails we will be thrown a ValueError because
	    # the object that is passed to json.loads is not valid JSON in which case we just continue
	    except (ValueError, KeyError):
		pass

	    k.set_contents_from_string(response)

	    # TOKEN STUFF FOR EACH USER
            # put the fbid,ownerid, and token in token_bucket
            # there may already be a token bucket key for this user so check first
            if not token_bucket.lookup(fbid):
                token_key = token_bucket.new_key()
                token_key.key = fbid
		all_tokens = tool.query("select ownerid, token from tokens where fbid={0}".format(fbid))
		cur_tokens = [(ownerid, token)]
		if len(all_tokens) > 0:
		    for each_set in all_tokens:
			cur_tokens.append((each_set[0], each_set[1]))
                token_key_struct = {"data": cur_tokens}
                jsoned = json.dumps(token_key_struct)
                token_key.set_contents_from_string(jsoned)
            else:
                token_key = token_bucket.get_key(fbid)
                # get the current tokens blob we have and convert it to a json object
                cur_tokens_blob = json.loads(token_key.get_contents_as_string())
                # check if this owner already has his/her token registered
                # some of our cur_tokens_blobs have "data" as their key instead of fbids
                try:
                    if ownerid in [x for x,y in cur_tokens_blob[fbid]]:
                        # if the key is still fbid copy its contents and delete it
                        cur_tokens_blob["data"] = cur_tokens_blob[fbid]
                        del cur_tokens_blob[fbid]
		    else:
			cur_tokens_blob[fbid].append((ownerid,token))
			cur_tokens_blob["data"] = cur_tokens_blob[fbid]
			del cur_tokens_blob[fbid]
                except KeyError:
		    if ownerid in [x for x,y in cur_tokens_blob["data"]]:
			pass
		    else:
			cur_tokens_blob["data"].append((ownerid,token))
	        # convert the current tokens blob back into a json string and put it back into the s3 fbtokens bucket
	
                cur_tokens_blob = json.dumps(cur_tokens_blob)
                token_key.set_contents_from_string(cur_tokens_blob)
            new_count += 1
	    print "%s added to s3" % main_key
        # otherwise we've already crawled our user and there should be information about
        # him/her in our main_bucket and our token_bucket
        else:
            # get everything from the subscribed updates with the next method's execution
            print "%s already crawled" % main_key
	
    return new_count


"""
    crawl_realtime_updates accesses all the realtime updates within the fbrealtime s3 bucket and crawls
    each pertaining user's feed based on the updated time provided by facebook.  the algorithm is smart
    enough to not re-crawl a user's feed that was previously crawled with always_crawl_from_database. 
    the aforementioned is accomplished with a crawled fbid log file that is read into crawl_realtime_updates
    and utilized.  once all the crawls have completed all the keys from fbrealtime s3 bucket that were
    used are deleted based on a starting time timestamp that is set upon the algorithms invocation
""" 




def crawl_realtime_updates(tool):
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    main_bucket = conn.get_bucket('fbcrawl1')
    token_bucket = conn.get_bucket('fbtokens')
    realtime_bucket = conn.get_bucket('fbrealtime')
    metric_bucket = conn.get_bucket('metric_bucket')
    api = 'https://graph.facebook.com/{0}?fields=feed.since({1})&access_token={2}'
    # REALTIME STUFF FROM FACEBOOK
    # get all the realtime update keys so we can parse through them and grab the updates
    _time = time.time()
    keys = []
    rs = realtime_bucket.list()
    for key in rs:
        keys.append(key.name)
    
    # keep track of which users we've crawled on this pass and make sure to not crawl
    # them twice or else we will have duplicate information in the database
    # users_crawled will also have pre-included fbids from the always_crawl_from_database algorithm
    # which generates a crawled log of fbids from it's execution in order to avoid duplicate crawling
    users_crawled = []
###################################################################################
    # this COULD be saving elsewhere and not being read in properly
    reader = csv.reader(open('crawl_log.csv','r'),delimiter=',')
    # read all the fbids from our file and add them to users_crawled
    try:
        while True:
            users_crawled.append(reader.next()[0])
    except StopIteration:
        pass
    for key in keys:
        k = realtime_bucket.get_key(key)
	try:
            info = json.loads(k.get_contents_as_string())

            # iterate through the list of [(fbid, updated_time), (fbid, updated_time)] 
            for fbid, update_time in info['data']:
                # if we haven't crawled this user yet on this pass....
                if fbid not in users_crawled:
                    try:
                        token_stuff = get_tokens_for_user(fbid,token_bucket)
                        try:
                            token_stuff = token_stuff["data"]
                        except KeyError:
                            token_stuff = token_stuff[fbid]
			    # lets preserve the C in ACID
			    token_stuff = {"data": token_stuff[fbid]}
		  
                    # if we don't have tokens for the user we get AttributeError because returns none, 
                    # get tokens put them into s3 and use them
                    except AttributeError:
                        fbid_tokens = tool.query("select ownerid, token from tokens where fbid='%s'" % fbid)
                        token_struct = {"data": []}
                       	token_stuff = []
                        try:
			    if len(fbid_tokens) > 0:
                                for each in fbid_tokens:
                                    # add (ownerid:token) to the struct
                                    token_struct["data"].append((each[0],each[1]))
                                    token_stuff.append((each[0],each[1]))
                            k = token_bucket.new_key()
                            k.key = fbid
                            k.set_contents_from_string(json.dumps(token_struct))
                        except TypeError:
                            token_stuff = None

                     
                    # for each pair of (ownerid,token) in the list of fbid's tokens...
                    # crawl his/her wall with each token and update the data in the main
                    # fbcrawl bucket
                    if token_stuff != None:
                        for ownerid, token in token_stuff:
                            # first let's get what we have on the current pair fbid,ownerid out of
                            # fbcrawl1 so we can add the new update stuff to it
                            main = str(fbid)+','+str(ownerid)
                            main_key = main_bucket.get_key(main)
			    post_ids = main_key.get_metadata('data')
                            # the data we already have
                            if main_key != None:
                                try:
				    # remember to call json.loads() twice
                                    cur_data = json.loads(main_key.get_contents_as_string())
				    cur_data = json.loads(cur_data)
                                except AttributeError:
                                    cur_data = ''
				
                            # the new data...we will add the old data to this for chronology purposes
                            graph_api = api.format(fbid,update_time,token)
                            try:
                                updated_stuff = json.loads(urllib2.urlopen(graph_api).read())
				# DELETE REPEAT POSTS
				repeats = [post for post in updated_stuff['feed']['data'] if post['id'] in post_ids]
				[updated_stuff['feed']['data'].remove(i) for i in repeats]
                                if cur_data != None:
                                    try:
                                        updated_stuff['feed']['data'] += cur_data['feed']['data']
                                        # error would be ValueError or KeyError
                                    except:
                                        updated_stuff = ''
                                # store the the data back where we got it with the new information added
                                main_key.set_contents_from_string(json.dumps(updated_stuff))
				post_ids = [post['id'] for post in updated_stuff['feed']['data'] if 'id' in post.keys()]
				main_key.set_metadata('data', post_ids) 
                                ############################################################
                                # run our metrics analysis on the new data and replace our old stuff
                                if metric_bucket.lookup(main):
                                    metric_object = metrics(updated_stuff)
                                    if metric_object != None:
                                        m_key = metric_bucket.get_key(main)
                                        m_key.set_contents_from_string(json.dumps(metric_object))
                                    else:
                                        pass
                                    # most likely a urllib2.HTTPError
                            except:
                                updated_stuff = ''	
                            main_key.set_contents_from_string(updated_stuff)
                    # add our fbid to users crawled since we've crawled him/her now
                users_crawled.append(fbid)
                print "User %s updated" % str(fbid)
            else:
                pass
        except:
		pass
    # delete all the obsolete keys that we've crawled that were created earlier than
    # this algorithm was invoked
    #realtime_bucket.delete_keys([key for key in realtime_bucket if int(key) < _time]) 
    delete_obsolete_keys(realtime_bucket, _time)
    print "Realtime updates added to s3"    


#######################################################################################################################################
# A NEW RAELTIME UPDATES

def crawl_realtime_updates2(tool):
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    main_bucket = conn.get_bucket('fbcrawl1')
    token_bucket = conn.get_bucket('fbtokens')
    realtime_bucket = conn.get_bucket('fbrealtime')
    metric_bucket = conn.get_bucket('metric_bucket')
    api = 'https://graph.facebook.com/{0}?fields=feed.since({1})&access_token={2}'
    # REALTIME STUFF FROM FACEBOOK
    # get all the realtime update keys so we can parse through them and grab the updates
    _time = time.time()
    keys = []
    rs = realtime_bucket.list()
        
    for key in rs:
            keys.append(key.name)
    
    # keep track of which users we've crawled on this pass and make sure to not crawl
    # them twice or else we will have duplicate information in the database
    # users_crawled will also have pre-included fbids from the always_crawl_from_database algorithm
    # which generates a crawled log of fbids from it's execution in order to avoid duplicate crawling
    users_crawled = []
###################################################################################
    # this COULD be saving elsewhere and not being read in properly
    reader = csv.reader(open('/home/ubuntu/crawl_stuff/crawl_log.csv','r'),delimiter=',')
    
    # read all the fbids from our file and add them to users_crawled
    try:
        while True:
            users_crawled.append(reader.next()[0])
    except StopIteration:
        pass
    for key in keys:
        k = realtime_bucket.get_key(key)
	try:
            info = json.loads(k.get_contents_as_string()) 
            # iterate through the list of [(fbid, updated_time), (fbid, updated_time)] 
            for fbid, update_time in info['data']:
                # if we haven't crawled this user yet on this pass....
                if fbid not in users_crawled:

                    token_stuff = get_tokens_for_user(fbid,token_bucket)
		   
		    try:
			token_stuff = json.loads(token_stuff)
                        try:
                            token_iterable = token_stuff["data"]
                        except KeyError:
                            token_iterable = token_stuff[fbid]
			    # lets preserve the C in ACID
			    token_stuff = {"data": token_stuff[fbid]}
		   
                    # if we don't have tokens for the user we get None back, 
                    # get tokens put them into s3 and use them
                    except ValueError:
                        fbid_tokens = tool.query("select ownerid, token from tokens where fbid='%s'" % str(fbid))
                        token_stuff = {"data": []}
                       	token_iterable = []
                       
			if len(fbid_tokens) > 0:
                            for each in fbid_tokens:
                                # add (ownerid:token) to the struct
                                token_stuff["data"].append((each[0],each[1]))
                                token_iterable.append((each[0],each[1]))
                            k = token_bucket.new_key()
                            k.key = fbid
                            k.set_contents_from_string(json.dumps(token_stuff))
                        else:
                            pass

                     
                    # for each pair of (ownerid,token) in the list of fbid's tokens...
                    # crawl his/her wall with each token and update the data in the main
                    # fbcrawl bucket
		       
                    if len(token_iterable) > 0:
                        for ownerid, token in token_iterable:
			   
			    try:	
                                # first let's get what we have on the current pair fbid,ownerid out of
                                # fbcrawl1 so we can add the new update stuff to it
                                main = str(fbid)+','+str(ownerid)
                                main_key = main_bucket.get_key(main)
			        post_ids = main_key.get_metadata('data')
                                # the data we already have
                                if main_key != None:
				    cur_data = main_key.get_contents_as_string()
			
				    try:
				        cur_data = json.loads(cur_data)
				        cur_data = json.loads(cur_data)
					
				        if post_ids != None:
					    post_ids = json.loads(post_ids)['data']
				        else:
					    # we don't need to assign post_ids = None because post_ids already == None
					    pass
				    except (TypeError, ValueError):
				        cur_data = None	
			        else:
				    main_key = main_bucket.new_key()
				    main_key.key = main
				    cur_data = None
				    post_ids = None
				
			    # IF THEY KEY DOESN'T EXIST (main_key = main_bucket.get_key(main) RETURNS NONE...)
			    # an AttributeError will be thrown by the execution of (cur_data = main_key.get_contents_as_string())
			    # because A NoneType doesn't have attributes
			    except AttributeError:	
				pass                        
				
                            # the new data...we will add the old data to this for chronology purposes
                            graph_api = api.format(fbid,update_time,token)

                            try:
                                try:
				    # if there actually isn't new stuff on the user's wall we will get
				    # {"id": fbid} returned
				    updated_stuff = json.loads(urllib2.urlopen(graph_api).read())
				     
				except urllib2.HTTPError:
				    updated_stuff = None
				# DELETE REPEAT POSTS
			
					
				try:
				    	
				    if post_ids != None and updated_stuff != None:
				        repeats = [post for post in updated_stuff['feed']['data'] if post['id'] in post_ids and 'id' in post.keys() and 'feed' in updated_stuff.keys() and 'data' in updated_stuff['feed'].keys()]
				        [updated_stuff['feed']['data'].remove(i) for i in repeats]
				    else:
				        pass
			        except (TypeError, KeyError):
				    pass 

				if cur_data != None:
                                    try:
				        # combine the list of posts into a new list that is ordered chronologically
				        # by adding the old posts to the end of the new posts list
				        
                                        updated_stuff['feed']['data'] += cur_data['feed']['data']
				    except KeyError:
					pass
                                     
                             	else:
				    updated_stuff = None
                                # store the the data back where we got it with the new information added
				
				if updated_stuff != None and main_key != None:
				    try:
				        post_ids = list(set([post['id'] for post in updated_stuff['feed']['data'] if 'id' in post.keys()]))
				        # remember to set_metadata before set_contents_from_string
					post_ids = {"data": post_ids}	
				        main_key.set_metadata('data', json.dumps(post_ids)) 
                              	        main_key.set_contents_from_string(json.dumps(updated_stuff))
											
                                        # run our metrics analysis on the new data and replace our old stuff
                                        if metric_bucket.lookup(main):
					    try:
                                                metric_object = metrics(updated_stuff)
                                                if metric_object != None:
                                                    m_key = metric_bucket.get_key(main)
                                                    m_key.set_contents_from_string(json.dumps(metric_object))
                                                else:
 					            pass
					    except:
						pass

				        else:
					    try:
					        metric_object = metrics(updated_stuff)
					        if metric_object != None:
						    m_key = metric_bucket.new_key()
						    m_key.key = main
						    m_key.set_contents_from_string(json.dumps(metric_object))
					        else:
						    pass
					    except:
					        pass

				    except (AttributeError, KeyError):
					pass 
				        ############################################################

				else:
				    pass
                                    # most likely a urllib2.HTTPError
                            except (urllib2.HTTPError, ValueError):
                                updated_stuff = ''	
                            #main_key.set_contents_from_string(updated_stuff)
                    # add our fbid to users crawled since we've crawled him/her now
                    users_crawled.append(fbid)
                    print "User %s updated" % str(main)
                else:
                    print "User %s was already crawled" % str(fbid)
        except:
	    print "User NOT updated"
    # delete all the obsolete keys that we've crawled that were created earlier than
    # this algorithm was invoked
    #realtime_bucket.delete_keys([key for key in realtime_bucket if int(key) < _time]) 
    delete_obsolete_keys(realtime_bucket, _time)
    print "Realtime updates added to s3"    
   

    

def delete_obsolete_keys(bucket,timestamp):
    for key in bucket:
        if not key.key.isdigit():
            bucket.delete_key(key)
    try:
        bucket.delete_keys([i for i in bucket.list() if int(i.key) < timestamp])
    except ValueError:
        delete_obsolete_keys(bucket,timestamp)
# we have an s3 bucket specifically for tokens so that when we've received an update from facebook
# about a user we can a) call this function to make sure we've got the token added and then
# b) call another function to use these tokens 

# {"fbid": [("owner1": "token1"), ("owner2": "token2"), ("owner3": "token3")]}

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
def get_tokens_for_user(fbid, bucket):
    fbid_tokens_data = bucket.get_key(fbid)
    #data = json.loads(fbid_tokens_data.get_contents_as_string())
    data = fbid_tokens_data.get_contents_as_string()
    return data
    #token_stuff = data[fbid]
    #return token_stuff


# relies on get_tokens_for_user list and also the RealTime updates data because these functions
# will be called immediately after an update is received upon the next crawl
def crawl_all_tokens(fbid, tokens, conn, update_time):
    crawl_data_bucket = conn.get_bucket('fbcrawl1')
    api = 'https://graph.facebook.com/{0}?fields=feed.since(%s)&access_token={1}' % update_time
    for ownerid,token in tokens:
        cur_key = crawl_data_bucket.get_key(fbid + ',' + ownerid)
        cur_key_data = json.loads(cur_key.get_contents_as_string())
        formatted = api.format(fbid,token)
        result = json.loads(urllib2.urlopen(formatted).read())
            
    
def crawl_feed(fbid,access_token):
    api = 'https://graph.facebook.com/{0}?fields=feed&access_token={1}'
    formatted = api.format(fbid,access_token)
    response = urllib2.urlopen(formatted)
    json_response = json.dumps(response.read())
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
