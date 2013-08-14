#!/usr/bin/env python

"""
    imetrics takes as parameter a json blob and a ownerid and returns an object with metrics around how connected
    the person whom's blob we are crawling and the owernid are by selectively retrieving and utilizing the posts,tags, etc.
    with the ownerid within that post somewhere whether it is in a "like" or a "comment" or a "post" that the ownerid
    actually made to the fbid's (target user) page or it is just a post the ownerid can be found in
"""


def imetrics(resp, ownerid):
    try:
        # posts from the cared about user period
        posts_from = [
			i for i in resp['feed']['data'] 
			if 'from' in i.keys() and ownerid == i['from']['id']
		     ]

        # generate a list of words from the message in each post that our cared user made 
        words_from_post = []
        for post in posts_from:
            if 'message' in post.keys():
                words_from_post.append(post['message'].split(' '))
	    if 'description' in post.keys():
		words_from_post.append(post['description'].split(' '))
	    

        # get all posts that the cared about user commented on
        comments_from = [
			    i for i in resp['feed']['data'] 
			    if 'comments' in i.keys() and i not in posts_from 
			    and ownerid in [
						i['comments']['data'][j]['from']['id'] 
						for j in range(len(i['comments']['data']))
				           ]
		        ]

        # get all posts that the cared about user liked
        likes_from = [
			i for i in resp['feed']['data'] 
			if 'likes' in i.keys() and i not in posts_from and i not in comments_from 
			and ownerid in [
						i['likes']['data'][j]['id'] 
						# got a key error here
						for j in range(len(i['likes']['data']))
				       ]
		     ]

        # get all posts that the user is tagged in in a story
        stories_with = [
			i for i in resp['feed']['data'] 
			if 'story_tags' in i.keys() and i not in likes_from and i not in comments_from and i not in posts_from
			and ownerid in [
						i['story_tags'][key][j]['id'] 
						for key in i['story_tags'].keys() 
						for j in range(len(i['story_tags'][key]))
				       ]
		       ]
        # story tags post with the cared about user in it and analysis on it
        story_words = []
        for post in stories_with:
            story_words += post['story'].split(' ')

    
        metric_object = {}
        metric_object["posts_from"] = {"posts": posts_from, "words": words_from_post}
        metric_object["comments_from"] = {"posts": comments_from, "words": words_from_comments}
        metric_object["likes_from"] = {"posts": likes_from, "words": words_from_likes}
        metric_object["stories_with"] = {"posts": stories_with, "words": words_from_stories}
        return metric_object

    except KeyError:
	return {}



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




