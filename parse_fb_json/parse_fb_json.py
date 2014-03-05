#!/usr/bin/python
import sys
import os
import json
from boto.s3.connection import S3Connection
from urlparse import urlparse
from tempfile import mkstemp
import argparse
import multiprocessing




AWS_ACCESS_KEY = "AKIAJDPO2KQRLOJBQP3Q"
AWS_SECRET_KEY = "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"
AWS_BUCKET_NAMES = [ "feed_crawler_%d" % i for i in range(5) ]




def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)

def feed_json_iter(bucket_names=AWS_BUCKET_NAMES):
    conn = get_conn_s3()
    for b, bucket_name in enumerate(bucket_names):
        sys.stderr.write("reading bucket %d/%d (%s)\n" % (b, len(bucket_names), bucket_name))

        for k, key in enumerate(conn.get_bucket(bucket_name).list()):
            # name should have format primary_secondary; e.g., "100000008531200_1000760833"
            prim_id, sec_id = map(int, key.name.split("_"))
            feed_raw = key.get_contents_as_string()
            #sys.stderr.write("\tread %d from %s\n" % (len(feed_raw), key.name))

            try:
                feed_json_list = json.loads(feed_raw)['data']
            except KeyError:
                sys.stderr.write("no data in feed %s\n" % key.name)
                continue

            sys.stderr.write("\t%d read feed json with %d posts from %s\n" % (k, len(feed_json_list), key.name))
            yield Feed(sec_id, feed_json_list)

class Feed(object):
    def __init__(self, user_id, feed_json_list):
        self.user_id = str(user_id)
        self.posts = []
        for post_json in feed_json_list:
            try:
                self.posts.append(FeedPost(post_json))
            except:
                sys.stderr.write("error parsing: " + str(post_json) + "\n\n")
                sys.stderr.write("full feed: " + str(feed_json_list) + "\n\n")
                raise

    def write(self, outfile_posts, outfile_links, delim="\t"):
        for p in self.posts:
            post_fields = [self.user_id, p.post_id, p.post_ts, p.post_type, p.post_app, p.post_from,
                      p.post_link, p.post_link_domain,
                      p.post_story, p.post_description, p.post_caption, p.post_message]
            post_line = delim.join([f.replace(delim, " ").encode('utf8', 'ignore') for f in post_fields])
            outfile_posts.write(post_line + "\n")

            for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                has_to =  "1" if user_id in p.to_ids else ""
                has_like = "1" if user_id in p.like_ids else ""
                has_comm = "1" if user_id in p.comment_ids else ""
                link_fields = [p.post_id, user_id, has_to, has_like, has_comm]
                link_line = "\t".join([f.encode('utf8', 'ignore') for f in link_fields])
                outfile_links.write(link_line + "\n")

    @staticmethod
    def write_labels(outfile_posts, outfile_links, delim="\t"):
        # these MUST match field order above
        post_fields = ['user_id', 'post_id', 'post_ts', 'post_type', 'post_app', 'post_from',
                       'post_link', 'post_link_domain',
                       'post_story', 'post_description', 'post_caption', 'post_message']
        outfile_posts.write(delim.join(post_fields) + "\n")

        link_fields = ['post_id', 'user_id', 'to', 'like', 'comment']
        outfile_links.write(delim.join(link_fields) + "\n")


class FeedPost(object):
    def __init__(self, post_json):
        self.post_id = str(post_json['id'])
        self.post_ts = post_json['updated_time']
        self.post_type = post_json['type']
        self.post_app = post_json['application']['id'] if 'application' in post_json else ""

        self.post_from = post_json['from']['id'] if 'from' in post_json else ""
        self.post_link = post_json.get('link', "")
        self.post_link_domain = urlparse(self.post_link).hostname if (self.post_link) else ""

        self.post_story = post_json.get('story', "")
        self.post_description = post_json.get('description', "")
        self.post_caption = post_json.get('caption', "")
        self.post_message = post_json.get('message', "")

        self.to_ids = set()
        self.like_ids = set()
        self.comment_ids = set()
        if ('to' in post_json):
            self.to_ids.update([user['id'] for user in post_json['to']['data']])
        if ('likes' in post_json):
            self.like_ids.update([user['id'] for user in post_json['likes']['data']])
        if ('comments' in post_json):
            self.comment_ids.update([user['id'] for user in post_json['comments']['data']])

def handle_feed(feed):
    fd_posts, filename_posts = mkstemp()
    fd_links, filename_links = mkstemp()
    with open(filename_posts, 'wb') as outfile_posts, open(filename_links, 'wb') as outfile_links:
        feed.write(outfile_posts, outfile_links)
    os.close(fd_posts)
    os.close(fd_links)
    return filename_posts, filename_links




###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    parser.add_argument('post_file', type=str, help='out file for feed posts')
    parser.add_argument('link_file', type=str, help='out file for user-post links (like, comm)')
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--maxfeeds', type=int, help='bail after x feeds are done', default=None)
    args = parser.parse_args()

    outfile_posts = open(args.post_file, 'wb')
    outfile_links = open(args.link_file, 'wb')

    Feed.write_labels(outfile_posts, outfile_links)

    pool = multiprocessing.Pool(args.workers)
    for i, (fn_posts, fn_links) in enumerate(pool.imap(handle_feed, feed_json_iter())):
        if (i % 1000 == 0):
            sys.stderr.write("\t%d\n" % i)
        outfile_posts.write(open(fn_posts).read())
        os.remove(fn_posts)
        outfile_links.write(open(fn_links).read())
        os.remove(fn_links)

        if (args.maxfeeds is not None) and (i >= args.maxfeeds):
            sys.exit("bailing")

