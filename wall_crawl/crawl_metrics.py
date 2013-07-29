#!/usr/bin/env python


"""
   metrics algorithm takes a json blob as a parameter and builds 4 criteria of metrics around it (posts, comments, likes, types)
   the posts metrics include a list of all users that have posted and then a list of tuples with (user, number_of_times_posted)
   the comments metrics include an average number of comments on posts and a list of tuples with (user, number_of_times_commented)
   the likes metrics include an average number of likes on posts and a list of tuples with (user, number_of_times_liked)
   the types metrics include a list of tuples with (type, number_of_times_type_of_post_on_feed)
"""



def metrics(blob):
     num_posts = len(blob['feed']['data'])
     # people who made posts including the user who's wall we're crawling
     posters = [blob['feed']['data'][e]['from']['id'] for e in range(len(blob['feed']['data'])) if 'from' in blob['feed']['data'][e].keys()]
     poster_counts = [(poster, posters.count(poster)) for poster in set(posters)]
     # like metrics including how many likes on average and those who like stuff and how often they do it   
     average_likes = sum([blob['feed']['data'][e]['likes']['count'] for e in range(len(blob['feed']['data'])) if 'likes' in blob['feed']['data'][e].keys()])/num_posts
     likers_unfiltered = [e['id'] for w in [blob['feed']['data'][i]['likes']['data'] for i in range(len(blob['feed']['data'])) if 'likes' in blob['feed']['data'][i].keys()] for e in w]
     likes_counts = [(liker, likers_unfiltered.count(liker)) for liker in set(likers_unfiltered)]
     # the types of posts that are on our user's wall and how many of each kind there are
     post_types = [blob['feed']['data'][e]['type'] for e in range(len(blob['feed']['data']))]
     type_counts = [(_type, post_types.count(_type)) for _type in set(post_types)]
     # returns list of [ ('status', 'mobile_status_update') ] tuples
     post_info = [(blob['feed']['data'][x]['type'], blob['feed']['data'][x]['status_type']) for x in range(len(blob['feed']['data'])) if 'type' in blob['feed']['data'][x].keys() and 'status_type' in blob['feed']['data'][x].keys()]
     # number of comments
     post_comments = [len(blob['feed']['data'][x]['comments']['data']) for x in range(len(blob['feed']['data'])) if 'comments' in blob['feed']['data'][x].keys()]
     average_comments = sum(post_comments)/len(post_comments)
     # get all of the ids of people who commented on statuses/posts
     comments = [blob['feed']['data'][x]['comments']['data'] for x in range(len(blob['feed']['data'])) if 'comments' in blob['feed']['data'][x].keys()]
     commenters_unfiltered = [comment_stack[w]['from']['id'] for comment_stack in comments for w in range(len(comment_stack))]
     commenter_count = [(commenter, commenters_unfiltered.count(commenter)) for commenter in set(commenters_unfiltered)]
     data_object = {}
     data_object['post_metrics'] = {'users': list(set(posters)), 'poster_count': poster_counts}
     data_object['like_metrics'] = {'average': average_likes, 'liker_count': likes_counts}
     data_object['type_metrics'] = {'count': type_counts}
     data_object['comment_metrics'] = {'average': average_comments, 'commenter_count': commenter_count}
     return data_object
