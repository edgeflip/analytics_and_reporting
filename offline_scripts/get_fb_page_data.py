#!/usr/bin/env python
#-*-coding:utf-8-*- 
#!/usr/bin/env python

"""
Facebook Graph API Explorer (http://developers.facebook.com/tools/explorer) 

GO HERE FOR DEFINITIONS OF VARIABLES RETURNED BY API: 
https://developers.facebook.com/docs/reference/api/post/

JSON Viewer (http://jsonviewer.stack.hu/)    
    
"""

token = "224759580885384|LWFtzBgXVgqhChOaT7vn0WNtFUg" #token from staging app

# import necessary Python libraries
import sys
import urllib
import string
import simplejson
import csv
import unicodecsv

import time
import datetime
from datetime import datetime, date, time
from pprint import pprint

from types import *

import re

# set up files tp write to
# need one file to store org level information (name, id, # likes, etc.) page_info
# need one file to store post level information (id of post, numver of likes, date pof post, etc.) post_meta
# need one file for likes (post_id, fbid of liker)
# need one file for comments (post_id, fbid of commenter, # likes on comment, text of comment, date, etc.)

generic_page_url = "https://graph.facebook.com/%s?access_token="+token
pagefile =  open("page_info.txt", 'wt')
pagewriter = unicodecsv.writer(pagefile)

generic_posts_url = "https://graph.facebook.com/%s/posts?access_token="+token+"&include_hidden=true" 
metafile = open ("post_meta.txt", 'wt')
metawriter = unicodecsv.writer(metafile)

likefile = open("page_likers.txt", 'wt')
likewriter = unicodecsv.writer(likefile)

commentfile = open("page_commenters.txt", 'wt')
commentwriter = unicodecsv.writer(commentfile)


def get_data(url):  
    try:
        data_object = simplejson.loads(urllib.urlopen(url).read())         
    except Exception, e:
        print "Error reading url %s, exception: %s" % (url, e)
        return None
    print "crawling url %s data_object.keys(): %s" % (url,data_object.keys())  
    return data_object


def write_page_data(data_object):   
        
    pagewriter.writerow([data_object['id'],data_object['name'],data_object['category'],data_object['likes'],data_object['link'],data_object['website'],data_object['talking_about_count'],datetime.now()])


def write_post_data(d):   
        
    posts = d['data']
    
    for post in posts:
        entire_post = str(post)
        org_name = post['from']['name']
        FB_org_id = post['from']['id']
                      
        if 'place' in post:
            location = str(post['place'])		
        else:
            location = ''
       
        if 'actions' in post:
            actions = str(post['actions'])
        else:
            actions = ''
                
        if 'link' in post:
        	link = post['link']      
        else:
        	link = ''
        
        if 'name' in post:
            link_name = post['name']
        else:
            link_name = ''
            
        if 'caption' in post:
            link_caption = post['caption']
        else:
            link_caption = ''
            
        if 'description' in post:
            link_description = post['description']
        else:
            link_description = ''
        
        if 'shares' in post:       
        	num_shares = post['shares']['count']

        if 'message' in post:
            content = post['message']
            content = content.replace('\n','') 
        else:
            content = ''
                
        last_comment = post['updated_time']
        
        published_date = post['created_time']
        post_type = post['type']	
        post_id = post['id']      
        org_id = post_id.split('_')[0]
        status_id = post_id.split('_')[1]
       
        
        status_link = 'https://www.facebook.com/%s/posts/%s' % (org_id, status_id) 
      
        if 'status_type' in post:
            status_type = post['status_type']
        else:
            status_type = ''

        if 'properties' in post:  
           properties = str(post['properties'])
        else:
            properties = ''
                      
        if 'application' in post:
            application = str(post['application'])
        else:
            application = ''
           
        if 'picture' in post:
            picture_link = post['picture']  
        else:
            picture_link = ''
        
        if 'source' in post:
            video_source = post['source'] 
        else:
            video_source = ''


        # start getting likes for post
        
        if 'likes' in post:
            for each_like in post ['likes']['data']:
                likerid = each_like['id']
                likewriter.writerow([FB_org_id,post_id,likerid, datetime.now()])
            print "Like Status: Done with first page of likes for %s" % post_id 
            
            if 'next' in post ['likes']['paging']:
                next_like_url = post ['likes']['paging']['next']
                while next_like_url:
                    print "crawling likes from %s" % next_like_url
                    try:
                        get_more_like = simplejson.loads(urllib.urlopen(next_like_url).read())
                        for more_likes in get_more_like['data']:
                            likerid = more_likes['id']
                            likewriter.writerow([FB_org_id,post_id,likerid])  
                    except Exception, e:
                        print "Error reading %s %s" % (next_like_url, e)
                    if 'paging' in get_more_like:
                            if 'next' in get_more_like['paging']:
                                next_like_url = get_more_like['paging']['next']
                                print "moving to next page of likes for %s url is %s" % (post_id,next_like_url)
                            else:
                                print "no more likes for %s" % post_id
                            break
                            
                
        # likes script ends

        print "starting to get comments"
        if 'comments' in post:
            for each_comment in post ['comments']['data']:
                comment = each_comment['message']
                commenterid= each_comment ['from']['id']
                commentdate = each_comment ['created_time']
                commentlikecount = each_comment ['like_count']
                commentwriter.writerow([FB_org_id,post_id,commenterid,commentdate,commentlikecount,comment])

            print "done with first page of comments"

            if 'next' in post ['comments']['paging']:
                next_comment_url = post ['comments']['paging']['next']
                print "crawling nextpage of comments at %s" % next_comment_url            
                while next_comment_url:
                    try:
                        get_more_comment = simplejson.loads(urllib.urlopen(next_comment_url).read())
                        for each_comment in post ['comments']['data']:
                            comment = each_comment['message']
                            commenterid= each_comment ['from']['id']
                            commentdate = each_comment ['created_time']
                            commentlikecount = each_comment ['like_count']
                            commentwriter.writerow([FB_org_id,post_id,commenterid,commentdate,commentlikecount,comment,datetime.now()])
                    except Exception, e:
                        print "Error reading %s %s" %(next_comment_url,e)

                    if 'paging' in get_more_comment:
                        if 'next' in get_more_comment['paging']:
                            next_comment_url = get_more_comment['paging']['next']
                            print "moving to next page of comments for %s url is %s" % (post_id,next_comment_url)
                        else:
                            print "no more comments for %s" % post_id
                            next_comment_url = ''
                    else:
                        next_comment_url = ''

            


        # Comments script ends
        mentions_list = []
        num_mentions = 0     
        if 'to' in post:
            num_mentions = len(post['to']['data'])
            if num_mentions !=0:
                mentions_list = [i['name'] for i in post['to']['data'] if 'name' in i]
            else:
				mentions_list = ''
            mentions = ', '.join(mentions_list)
        else:
            mentions = ''
        
        metawriter.writerow([org_name, FB_org_id,location, link, post_id, org_id, status_id, status_link, content, published_date, last_comment, post_type, status_type, video_source, picture_link, link_name, link_caption, link_description, num_mentions, mentions, actions, application, properties, datetime.now()])
               
        
        
def main():
    pages_to_crawl = ['Advocates4Youth','nonprofits'] #if a page name contains multiple words, separated by space (e.g. SPOT Coffee); it will show up as words connected by hyphens in URL. For example, SPOT Cofee as in "Spot-Coffee-Elmwood." If so, it is recommended that you use page id. You can find page id in the URL - it is the string of numbers after page name in URL.  (e.g. https://www.facebook.com/pages/Spot-Coffee-Elmwood/316579834919) 
    
    for page_name in pages_to_crawl:
        print "Main Status: Starting page crawl for %s" % page_name
        
        # get page level data
        page_url = generic_page_url % (page_name)

        page_info = get_data(page_url)
        
        #write page levle data
        write_page_data(page_info)

        posts_url = generic_posts_url % (page_name)      
        
        while posts_url:

            # get post level data
            d = get_data(posts_url)
        
            #errors
            if not d:
                print "THERE WAS NO 'D' RETURNED by %s ........MOVING TO NEXT ID" % page_name
                break						##### RETURN TO THE BEGINNING OF THE LOOP          
            if not 'data' in d:
                print "THERE WAS NO 'D['DATA']' RETURNED by %s ........MOVING TO NEXT ID" % page_name				#
                break						##### RETURN TO THE BEGINNING OF THE LOOP    
            if len(d['data'])==0:
                print "THERE WERE NO STATUSES RETURNED by %s ........MOVING TO NEXT ID" % page_name
                break						##### RETURN TO THE BEGINNING OF THE LOOP    
            
            # writing post level data for first page of posts
            write_post_data(d)  
            paging = d['paging']

            if 'next' in paging:
                posts_url = paging['next']
                print "NEXT PAGE URL:", posts_url
            else:
                posts_url=''
                print "NO NEXT PAGE FOR", feed

        print "Main Status: Done with getting posts for %s" %(page_name)



if __name__ == "__main__":
    main()
