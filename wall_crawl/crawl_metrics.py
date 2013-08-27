#!/usr/bin/env python
from con_s3 import connect_s3
import json

"""
    build_weighted connects to our metric_bucket and builds metrics around what we have already
    created during the main crawler systemized data collection process.  this algorithm will
    leverage previously stored data and advance our understanding of the edges contained
    therein
"""

def build_weighted():
    conn = connect_s3()
    mets = conn.get_bucket('metric_bucket')
    for key in mets.list():
        cur_mets = json.loads(key.get_contents_as_string())
        ownerid = key.key
        # get all the posts_ids 
        post_ids = []
        for fbid in cur_mets.keys():
            if fbid != 'weights':
                if 'prim_to_sec' in cur_mets[fbid].keys():
                    for each in cur_mets[fbid]['prim_to_sec'].keys():
                        for post in cur_mets[fbid]['prim_to_sec'][each]:
                            if isinstance(post, dict):
                                if post['id'] not in post_ids:
                                    post_ids.append(post['id'])
                else:
                    for each in cur_mets[fbid].keys():
                        for post in cur_mets[fbid][each]:
                            if isinstance(post, dict):
                                if post['id'] not in post_ids:
                                    post_ids.append(post['id']) 
            else:
                pass
       
        divisor = float(len(post_ids))
        cur_weights = {}
        for fbid in cur_mets.keys():
            if fbid != ownerid and fbid != 'weights':
                if 'prim_to_sec' in cur_mets[fbid].keys():
                    numerator = float( sum( [ len( cur_mets[fbid]['prim_to_sec'][k] ) for k in cur_mets[fbid]['prim_to_sec'].keys() ] ) )
                    numerator = numerator / 2.0
                    if 'sec_to_prim' in cur_mets[fbid].keys():
                        numerator_2 = float( sum( [ len( cur_mets[fbid]['sec_to_prim'][k] ) for k in cur_mets[fbid]['sec_to_prim'].keys() ] ) )
                        numerator_2 = numerator_2 / 2.0
                        numerator += numerator_2
                
                else:
                    numerator = float( sum( [ len( cur_mets[fbid][k] ) for k in cur_mets[fbid].keys() ] ) )
                    numerator = numerator / 2.0
                n = numerator / divisor
                cur_weights[str(n)] = fbid
            else:
                pass
        cur_mets["weights"] = cur_weights

        key.set_contents_from_string(json.dumps(cur_mets))
    	print "Connections of %s weighed" % str(ownerid)
    print "All connections weighed"


"""
   build_weighted_test runs a single requested metric_bucket key and creates the weighted
   graph of that key within our metric_bucket (in more abstract words the build_weighted_test
   algorithm advances our known edge relations between a primary and his/her secondaries
   based on the posts, likes, comments, stories tagged they have in common
"""

def build_weighted_test(ownerid):
    conn = connect_s3()
    metrics = conn.get_bucket('metric_bucket')
    key = metrics.get_key(ownerid)
    if key != None:
        cur_mets = json.loads(key.get_contents_as_string())
        # get all the posts_ids 
        post_ids = []
        for fbid in cur_mets.keys():
            if fbid != 'weights':
                if 'prim_to_sec' in cur_mets[fbid].keys():
                    for each in cur_mets[fbid]['prim_to_sec'].keys():
                        for post in cur_mets[fbid]['prim_to_sec'][each]:
                            if isinstance(post, dict):
                                if post['id'] not in post_ids:
                                    post_ids.append(post['id'])
                else:
                    for each in cur_mets[fbid].keys():
                        for post in cur_mets[fbid][each]:
                            if isinstance(post, dict):
                                if post['id'] not in post_ids:
                                    post_ids.append(post['id']) 
            else:
                pass
        divisor = float(len(post_ids))
        cur_weights = {}
        for fbid in cur_mets.keys():
            if fbid != ownerid and fbid != 'weights':
                if 'prim_to_sec' in cur_mets[fbid].keys():
                    numerator = float( sum( [ len( cur_mets[fbid]['prim_to_sec'][k] ) for k in cur_mets[fbid]['prim_to_sec'].keys() ] ) )
                    numerator = numerator / 2.0
                    if 'sec_to_prim' in cur_mets[fbid].keys():
                        numerator_2 = float( sum( [ len( cur_mets[fbid]['sec_to_prim'][k] ) for k in cur_mets[fbid]['sec_to_prim'].keys() ] ) )
                        numerator_2 = numerator_2 / 2.0
                        numerator += numerator_2
                
                else:
                    numerator = float( sum( [ len( cur_mets[fbid][k] ) for k in cur_mets[fbid].keys() ] ) )
                    numerator = numerator / 2.0
                n = numerator / divisor
                cur_weights[str(n)] = fbid
            else:
                pass
        cur_mets["weights"] = cur_weights
 
             
        key.set_contents_from_string(json.dumps(cur_mets))
        print "Connections of %s weighed" % str(ownerid)
    else:
        print "Connections of %s not weight" % str(ownerid)

            



"""
    imetrics takes as parameter a json blob and a ownerid and returns an object with metrics around how connected
    the person whom's blob we are crawling and the owernid are by selectively retrieving and utilizing the posts,tags, etc.
    with the ownerid within that post somewhere whether it is in a "like" or a "comment" or a "post" that the ownerid
    actually made to the fbid's (target user) page or it is just a post the ownerid can be found in
"""


def imetrics(resp, ownerid):
    try:
        # posts from the cared about user period
        posts_from = []
        for i in resp['feed']['data']:
            if 'from' in i.keys() and ownerid == i['from']['id']:
                posts_from.append(i)

        # get all posts that the cared about user commented on
        comments_from = []
        for i in resp['feed']['data']:
            if 'comments' in i.keys() and i not in posts_from and ownerid in [i['comments']['data'][j]['from']['id'] for j in range(len(i['comments']['data']))]:
                comments_from.append(i)

        # get all posts that the cared about user liked
        likes_from = []
        for i in resp['feed']['data']:
            if 'likes' in i.keys() and i not in posts_from and i not in comments_from and ownerid in [i['likes']['data'][j]['id'] for j in range(len(i['likes']['data']))]:
                likes_from.append(i)

        # get all posts that the user is tagged in in a story
        stories_with = []
        for i in resp['feed']['data']:
            if 'story_tags' in i.keys() and i not in likes_from and i not in comments_from and i not in posts_from and ownerid in [i['story_tags'][key][j]['id'] for key in i['story_tags'].keys() for j in range(len(i['story_tags'][key]))]:
                stories_with.append(i)
 
        metric_object = {}
        metric_object["posts_from"] = posts_from
        metric_object["comments_from"] = comments_from
        metric_object["likes_from"] = likes_from
        metric_object["stories_with"] = stories_with
	
        return metric_object

    except KeyError:
	return None


def ometrics(ownerid, fbid, conn):
    try:
        wall = json.loads(conn.get_bucket('fbcrawl1').get_key(ownerid + ',' + ownerid).get_contents_as_string())
        try:
            wall = json.loads(wall)
        except TypeError:
            pass
        result = imetrics(wall, fbid)
        return result
    except (AttributeError, TypeError):
        return None

"""
   gmetrics algorithm takes a json blob as a parameter and builds 4 criteria of metrics around it (posts, comments, likes, types)
   the posts metrics include a list of all users that have posted and then a list of tuples with (user, number_of_times_posted)
   the comments metrics include an average number of comments on posts and a list of tuples with (user, number_of_times_commented)
   the likes metrics include an average number of likes on posts and a list of tuples with (user, number_of_times_liked)
   the types metrics include a list of tuples with (type, number_of_times_type_of_post_on_feed)
"""



def gmetrics(blob):
     if blob == '':
         return None
     else:
         num_posts = len(blob['feed']['data'])
         # people who made posts including the user who's wall we're crawling
         posters = [
		       blob['feed']['data'][e]['from']['id'] 
		       for e in range(len(blob['feed']['data'])) 
		       if 'from' in blob['feed']['data'][e].keys()
		   ]

         poster_counts = [(poster, posters.count(poster)) for poster in set(posters)]

         # like metrics including how many likes on average and those who like stuff and how often they do it   
         try:
             average_likes = sum([
				    blob['feed']['data'][e]['likes']['count'] 
				    for e in range(len(blob['feed']['data'])) 
				    if 'likes' in blob['feed']['data'][e].keys()
				 ]) / num_posts

         except ZeroDivisionError:
             average_likes = 0

         likers_unfiltered = [
				e['id'] for w in [blob['feed']['data'][i]['likes']['data'] 
				for i in range(len(blob['feed']['data'])) 
				if 'likes' in blob['feed']['data'][i].keys()] 
				for e in w
			     ]

         likes_counts = [
			    (liker, likers_unfiltered.count(liker)) 
			    for liker in set(likers_unfiltered)
		  	]

         # the types of posts that are on our user's wall and how many of each kind there are
         post_types = [	
			blob['feed']['data'][e]['type'] 
			for e in range(len(blob['feed']['data']))
		      ]

         type_counts = [(_type, post_types.count(_type)) for _type in set(post_types)]
         # returns list of [ ('status', 'mobile_status_update') ] tuples
         post_info = [
			(blob['feed']['data'][x]['type'], blob['feed']['data'][x]['status_type']) 
			for x in range(len(blob['feed']['data'])) 
			if 'type' in blob['feed']['data'][x].keys() and 'status_type' in blob['feed']['data'][x].keys()
		     ]

         # number of comments
         post_comments = [
			    len(blob['feed']['data'][x]['comments']['data']) 
			    for x in range(len(blob['feed']['data'])) 
			    if 'comments' in blob['feed']['data'][x].keys()
			 ]

         try:
             average_comments = sum(post_comments)/len(post_comments)
         except ZeroDivisionError:
             average_comments = 0
         # get all of the ids of people who commented on statuses/posts
         comments = [
			blob['feed']['data'][x]['comments']['data'] 
			for x in range(len(blob['feed']['data'])) 
			if 'comments' in blob['feed']['data'][x].keys()
		    ]

         commenters_unfiltered = [
				    comment_stack[w]['from']['id'] 
				    for comment_stack in comments 
				    for w in range(len(comment_stack))
				 ]

         commenter_count = [(commenter, commenters_unfiltered.count(commenter)) for commenter in set(commenters_unfiltered)]
         data_object = {}
         data_object['post_metrics'] = {'users': list(set(posters)), 'poster_count': poster_counts}
         data_object['like_metrics'] = {'average': average_likes, 'liker_count': likes_counts}
         data_object['type_metrics'] = {'count': type_counts}
         data_object['comment_metrics'] = {'average': average_comments, 'commenter_count': commenter_count}
         return data_object




