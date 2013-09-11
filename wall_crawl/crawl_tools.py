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
from crawl_metrics import gmetrics as metrics
from crawl_metrics import imetrics, ometrics


"""
    using the imetrics algorithm from crawl_metrics.py we will build metrics for our users in a paralell
    manner simultaneously to updating their feeds in the s3 bucket.  The process will be as follows:
    1) when always_crawl_from_database runs we get each user's feed and simultaenously pass that feed to metrics
       to build a metric object around, then we will store both objects (feed and metrics) in their respective
       locations
    2) when crawl_realtime_updates runs we get each pertinent user's updates and add them to the feed that we
       currently have. within the same code execution we will pass the NEW feed with updates to metrics and
       get back an updated metric for the pertinent user at which point we will then just replace any metric
       we had stored for him/her
"""


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
    # connect to s3 database
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    # bucket with all facebook edge feeds key = fbid,ownerid val = {facebook feed}
    main_bucket = conn.get_bucket('fbcrawl1')
    # bucket with all of a fbid's tokens ( 1 of two solutions 1) create function to periodically keep this updated 2) forget it and just hit db every time )
    token_bucket = conn.get_bucket('fbtokens')
    # bucket facebok realtime API hits with {fbid: update time, fbid: updated time }...
    realtime_bucket = conn.get_bucket('fbrealtime')
    # each key is a primary's fbid with { secondary: metrics, secondary2: metrics, .... secondary_n: metrics }
    metric_bucket = conn.get_bucket('metric_bucket') 
    # bucket for crawl_log
    crawl_log = conn.get_bucket('someobscenebucketname')
    # we need to set a limit and offset so as to not eat up all our memory
    # we will crawl in batches of 10000
    l = 10000
    o = 0
    if not crawl_timestamp:
        most_data = tool.query('select fbid,ownerid,token from tokens limit {0} offset {1}'.format(l, o))
    else:
        most_data = tool.query('select fbid,ownerid,token from tokens where updated > FROM_UNIXTIME({0}) limit {1} offset {2}'.format(crawl_timestamp, limit, offset))
    
    while len(most_data) > 0: 
        new_count = 0
        for item in most_data:
            fbid = str(item[0])
            ownerid = str(item[1])
            token = item[2]
            main_key = str(fbid)+','+str(ownerid)
            # if there is no key for this edge fbid,ownerid then we'll crawl it
            if not main_bucket.lookup(main_key):
                # go ahead and write the fbid to the someobscenebucketname bucket
                cl = crawl_log.new_key()
                cl.key = fbid
                cl.set_contents_from_string('already crawled')
                # crawl_feed returns a json blob of the users feed
                # on this pass of the code we are getting the entire feed    
                response = crawl_feed(fbid, token)
                k = main_bucket.new_key()
                # set the bucket's key to be fbid,ownerid
                k.key = main_key

    	        # META DATA FOR POST IDS TO AVOID DUPLICATE POSTS IN THIS FEED
                # you MUST set metadata before setting the key's value otherwise the metadata won't be saved (weird s3 rule)
                # we need to call json.loads() on response twice depending on the encoding
                if response != '': 
                    response = json.loads(response)
                    try:
                        response = json.loads(response)
                    except (ValueError, TypeError):
                        pass
       
                    try:
		        post_ids = list(set([each['id'] for each in response['feed']['data'] if 'id' in each.keys()]))
                    except KeyError:
                        post_ids = []
                    # SET THE META DATA
                    the_data = {'data': post_ids}
                    k.set_metadata('data', json.dumps(the_data))

                    # METRICS PORTION
	   
                    # there are times when this will return a TypeError or KeyError depending on what the response looks like
                    # that is passed to the metrics algorithm
                    try:
                        if fbid != ownerid:
                            # find all posts in the fbid's wall that ownerid posted, liked, commented on, and tagged in
                            # returns a dict {"stories_with": [ posts ], "comments_from": [ posts ], "likes_from": [ posts ], "posts_from": [ posts ] }
                            metric_obj = imetrics(response,ownerid)
                            if metric_obj != None:
                                if not metric_bucket.lookup(ownerid):
                                    m_key = metric_bucket.new_key()
                                    m_key.key = ownerid
                                    cur_met_obj = {fbid: {"prim_to_sec": metric_obj } }
                                    omets = ometrics(fbid, ownerid, main_bucket) 
                                    if omets != None:
                                        cur_met_obj[fbid]["sec_to_prim"] = omets
                                        
                                else:
                                    m_key = metric_bucket.get_key(ownerid)
                                    cur_met_obj = json.loads(m_key.get_contents_as_string())
             
                                    cur_met_obj[fbid] = {"prim_to_sec": metric_obj}
                                    omets = ometrics(ownerid, fbid, main_bucket)
                                    if omets != None:
                                        cur_met_obj[fbid]["sec_to_prim"] = omets
                                    else: 
                                        pass

                                m_key.set_contents_from_string(json.dumps(cur_met_obj))
			   
                            else:
                                pass
                        else:
                            pass

               	    except (TypeError, KeyError):
                        pass

                else:
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
                    # set the time crawled in the data structure so that we can use it in the future
                    token_key_struct["timestamp"] = int(time.time())
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
                    # keep our tokens blob updated
                    cur_tokens_blob = keep_tokens_updated(fbid, cur_tokens_blob, tool)
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
       
        o += l	
        if not crawl_timestamp:
            most_data = tool.query('select fbid,ownerid,token from tokens limit {0} offset {1}'.format(l, o))
        else:
            most_data = tool.query('select fbid,ownerid,token from tokens where updated > FROM_UNIXTIME({0}) limit {1} offset {2}'.format(crawl_timestamp, limit, offset))
    return new_count


"""
    crawl_realtime_updates accesses all the realtime updates within the fbrealtime s3 bucket and crawls
    each pertaining user's feed based on the updated time provided by facebook.  the algorithm is smart
    enough to not re-crawl a user's feed that was previously crawled with always_crawl_from_database. 
    the aforementioned is accomplished with a crawled fbid log file that is read into crawl_realtime_updates
    and utilized.  once all the crawls have completed all the keys from fbrealtime s3 bucket that were
    used are deleted based on a starting time timestamp that is set upon the algorithms invocation
""" 

import pdb
def crawl_realtime_updates(tool):
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    main_bucket = conn.get_bucket('fbcrawl1')
    token_bucket = conn.get_bucket('fbtokens')
    realtime_bucket = conn.get_bucket('fbrealtime')
    metric_bucket = conn.get_bucket('metric_bucket')
    crawl_log = conn.get_bucket('someobscenebucketname')
    # REALTIME STUFF FROM FACEBOOK
    # get all the realtime update keys so we can parse through them and grab the updates
    _time = time.time()
    keys = []
    rs = realtime_bucket.list()
    pdb.set_trace()
    for k in rs:
        try:
            info = json.loads(k.get_contents_as_string()) 
            realtime_bucket.delete_key(k)
            # iterate through the list of [(fbid, updated_time), (fbid, updated_time)] 
            for fbid, update_time in info['data']:
                # if we haven't crawled this user yet on this pass....
                if not crawl_log.lookup(fbid):
                    key = crawl_log.new_key()
                    key.key = fbid
                    key.set_contents_from_string('already crawled')
                    token_stuff = get_tokens_for_user(fbid,token_bucket)
		   
                    if token_stuff != None:
                        token_stuff = json.loads(token_stuff)
                        # make sure our token_stuff is up to date
                        token_stuff = keep_tokens_updated(fbid, token_stuff, tool)
                        try:
                            token_iterable = token_stuff["data"]
                      
                        except KeyError:
                            token_iterable = token_stuff[fbid]
                            # lets preserve the Consistency in ACID
       	                    token_stuff = {"data": token_stuff[fbid]}
                        # if we don't have a timestamp let's make one
                        if "timestamp" not in token_stuff.keys():
                            token_stuff["timestamp"] = time.time()
		      
                    # if we don't have tokens for the user we get None back, 
                    # get tokens put them into s3 and use them
                    else:
                        fbid_tokens = tool.query("select ownerid, token from tokens where fbid='%s'" % str(fbid))
                        token_stuff = {"data": []}
                        token_stuff["timestamp"] = time.time()
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
                                # the data we already have
                                if main_key != None: 
                                    # get our post_ids so that we can remove avoid redundancy 
                                    post_ids = main_key.get_metadata('data') 
                                    cur_data = main_key.get_contents_as_string()	
                                    try:
                                        cur_data = json.loads(cur_data)
                                        # depending on the encoding, we may have to call json.loads() twice
                                        try:
                                            cur_data = json.loads(cur_data)
                                        except TypeError:
                                            pass
					
                                        if post_ids != None:
                                            try:
                                                post_ids = json.loads(post_ids)['data']
                                            except (TypeError, ValueError, KeyError):
                                                pass
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
				
                            # the new data...we will add the old data to this for persistent chronological order

			    # crawl_feed_since will handle any errors associated with the crawl
                            if cur_data == None:
                                updated_stuff = crawl_feed(fbid, token)
                            else:
                                updated_stuff = crawl_feed_since(fbid, update_time, token)
                            if updated_stuff != None:
                                updated_stuff = json.loads(updated_stuff)
                                # again ensuring we have a dict in-hand to work with instead of a unicode
                                try: 
                                    updated_stuff = json.loads(updated_stuff)
                                except:
                                    pass
                            # DELETE REPEAT POSTS					
                            if post_ids != None and updated_stuff != None:
                                try:
                                    repeats = [post for post in updated_stuff['feed']['data'] if post['id'] in post_ids and 'id' in post.keys() and 'feed' in updated_stuff.keys() and 'data' in updated_stuff['feed'].keys()]
                                    [updated_stuff['feed']['data'].remove(i) for i in repeats]
                                except KeyError:
                                    pass
                            else:
                                pass

                            if cur_data != None and updated_stuff != None:
                                try:
                                    # combine the list of posts into a new list that is ordered chronologically
                                    # by adding the old posts to the end of the new posts list				        
                                    updated_stuff['feed']['data'] += cur_data['feed']['data']
                                except KeyError:
                                    pass 
                            else:
                                pass
                                # store the the data back where we got it with the new information added
				
                            if updated_stuff != None and main_key != None:
                                try:
                                    post_ids = list(set([post['id'] for post in updated_stuff['feed']['data'] if 'id' in post.keys() and 'feed' in updated_stuff.keys()]))
                                    # remember to set_metadata before set_contents_from_string otherwise it won't stick (weird s3 rule)
                                    post_ids = {"data": post_ids}	
                                    main_key.set_metadata('data', json.dumps(post_ids)) 
                              	    main_key.set_contents_from_string(json.dumps(updated_stuff))
											
                                    # run our metrics analysis on the new data and replace our old stuff
                                    if metric_bucket.lookup(ownerid):
                                        try:
                                            if fbid != ownerid:
                                                # finds all of the relevant posts that the ownerid made/is in in this user's data
                                                metric_object = imetrics(updated_stuff, ownerid)
                                                if metric_object != None:
                                                    # we will store these in the metric bucket by ownerid and have all fbids
                                                    # that are connections as keys in a json object
                                                    m_key = metric_bucket.get_key(ownerid)
                                                    cur_met_blob = m_key.get_contents_as_string()
                                                    if cur_met_blob != None:
                                                        cur_met_blob = json.loads(cur_met_blob)
                                                        # the imetrics algorithm we ran gets the "primary to secondary" connectivity
                                                        # we will need to run the ometrics algorithm to get "secondary to primary"
                                                        # connectivity 
                                                        cur_met_blob[fbid] = {"prim_to_sec": metric_object}
                                                        omets = ometrics(ownerid, fbid, main_bucket)
                                                        if omets != None:
                                                            cur_met_blob[fbid]["sec_to_prim"] = omets
					           
                                                    else:
                                                        # the metric_bucket didn't have an object for this ownerid...so we will
                                                        # build and store that object with almost identical semantics
                                                        cur_met_blob = {fbid: {"prim_to_sec": metric_object }}
                                                        omets = ometrics(ownerid, fbid, main_bucket)
                                                        if omets != None:
                                                            cur_met_blob[fbid]["sec_to_prim"] = omets
					        
                                                        m_key.set_contents_from_string(json.dumps(cur_met_blob))
                                
                                                else:
                                                    pass
                                            else:
                                                pass
                                        except:
					    pass

                                    else:
                                        metric_object = imetrics(updated_stuff, ownerid) 
                                        if metric_object != None:
				            m_key = metric_bucket.new_key()
                                            m_key.key = ownerid
                                            cur_met_blob = {fbid: {"prim_to_sec": metric_object } }
                                            omets = ometrics(ownerid, fbid, main_bucket)
                                            if omets != None:
                                                cur_met_blob[fbid]["sec_to_prim"] = omets
                                            m_key.set_contents_from_string(json.dumps(cur_met_blob))
                                        else:
                                            pass 

                                except (AttributeError, KeyError):
                                    pass 
				        ############################################################
                            else:
                                pass

                        print "User %s updated" % str(main)
                else:
                    print "User %s was already crawled" % str(fbid)
        except:
            print "User NOT updated"
    # delete our crawl_log as it only pertains to the execution on this pass 
    cl = crawl_log.list()
    for key in cl:
        crawl_log.delete_key(key)
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
        for i in bucket.list():
            if int(i.key) < timestamp:
                bucket.delete_key(i)
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
    try:
        data = fbid_tokens_data.get_contents_as_string()
    except AttributeError:
        data = None
    return data


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
    #api = 'https://graph.facebook.com/{0}?fields=feed&access_token={1}'
    api = 'https://graph.facebook.com/{0}?fields=feed.fields(id,from,message,type,link,source,name,status_type,application,story_tags,story,caption,created_time,updated_time,likes.fields(id).limit(100),comments.fields(id,from,message,like_count).limit(100))&access_token={1}'
    formatted = api.format(fbid,access_token)
    try:
        response = urllib2.urlopen(formatted)
        json_response = json.dumps(response.read())
    except (urllib2.URLError, urllib2.HTTPError):
        json_response = ''
    return json_response

def crawl_feed_since(fbid, since, access_token):
    #api = 'https://graph.facebook.com/{0}?fields=feed.since({1})&access_token={2}'
    api = 'https://graph.facebook.com/{0}?fields=feed.fields(id,from,message,type,link,source,name,status_type,application,story_tags,story,caption,created_time,updated_time,likes.fields(id).limit(100),comments.fields(id,from,message,like_count).limit(100)).since({1})&access_token={2}'
    formatted = api.format(fbid, str(since), access_token)
    try:
        response = urllib2.urlopen(formatted)
        json_response = json.dumps(response.read())
    except (urllib2.URLError, urllib2.HTTPError):
        json_response = None
    return json_response


def check_if_crawled(fbid,friend_fbid):
    from boto.s3.connection import S3Connection
    from boto.s3.key import Key
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    bucket = conn.get_bucket('fbcrawl1')
    if bucket.lookup('{0},{1}'.format(fbid,friend_fbid)):
        return True
    return False

# a method to call on our tokens blob to keep it updated
def keep_tokens_updated(fbid, blob, tool):    
    if "timestamp" in blob.keys():
        if blob["timestamp"] < (time.time() - 86400):
           res = tool.query("select ownerid, token from tokens where fbid='%s'" % fbid)
           if len(res) > 0:
               for ownerid, token in res:
                   if ownerid not in [ ownerid for ownerid, token in blob["data"] ]:
                       blob["data"].append((ownerid, token))
        else:
           pass
        return blob
    else:
        return blob 


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


"""
    takes as parameter an s3 connection and returns a consolidated version of our
    realtime updates bucket while simultaneously cleaning it up.  when we get multiple
    updates for a given fbid in realtime updates we really only care about the earliest
    one and not the subsequent updates because they are less rich with data as well
    as redundant
"""

def choose_best(conn):
    fbid_to_times = { }
    realtime = conn.get_bucket('fbrealtime')
    rl = realtime.list()
    for key in rl:
        # returns us our {'data': [ [fbid, update_time], [fbid, update_time] ]} hash table
        data = json.loads(key.get_contents_as_string())
        for fbid, update_time in data['data']:
            if fbid not in fbid_to_times.keys():
                fbid_to_times[fbid] = [update_time]
            else:
                fbid_to_times[fbid].append(update_time)
    fbid_to_times = [ (fbid, max(fbid_to_times[fbid])) for fbid in fbid_to_times.keys() ]
    return fbid_to_times
