#!/usr/bin/env python

def metrics(blob):
     num_posts = len(blob['feed']['data'])
     # people who made posts including the user who's wall we're crawling
     posters = [blob['feed']['data'][e]['from']['id'] for e in range(len(blob['feed']['data'])) if 'from' in blob['feed']['data'][e].keys()]
     poster_counts = [(poster, posters.count(poster)) for poster in set(posters)]
     # like metrics including how many likes on average and those who like stuff and how often they do it   
     average_likes = sum([blob['feed']['data'][e]['likes']['count'] for e in range(len(blob['feed']['data'])) if 'likes' in blob['feed']['data'][e].keys()])/num_posts
     likers = list(set([e['id'] for w in [blob['feed']['data'][i]['likes']['data'] for i in range(len(blob['feed']['data'])) if 'likes' in blob['feed']['data'][i].keys()] for e in w]))
     likes_counts = [(liker, likers.count(liker)) for liker in likers]
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
     comment_ids = list(set([comment_stack[w]['from']['id'] for comment_stack in comments for w in range(len(comment_stack))]))
     data_object = {}
     data_object['post_metrics'] = {'users': list(set(posters)), 'count': poster_counts}
     data_object['like_metrics'] = {'average': average_likes, 'users': likers}
     data_object['type_metrics'] = {'count': type_counts, 'types': list(set(post_info))}
     data_object['comment_metrics'] = {'average': average_comments, 'users': comment_ids}
     return data_object
