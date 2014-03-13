#!/usr/bin/python
import sys
import os
import json
import logging
from boto.s3.connection import S3Connection
from urlparse import urlparse
import tempfile
import argparse
import multiprocessing
import time




AWS_ACCESS_KEY = "AKIAJDPO2KQRLOJBQP3Q"
AWS_SECRET_KEY = "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"
AWS_BUCKET_NAMES = [ "feed_crawler_%d" % i for i in range(5) ]




logger = logging.getLogger(__name__)

def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)

def key_iter(bucket_names=AWS_BUCKET_NAMES):
    conn = get_conn_s3()
    for b, bucket_name in enumerate(bucket_names):
        logger.debug("reading bucket %d/%d (%s)\n" % (b, len(bucket_names), bucket_name))
        for key in conn.get_bucket(bucket_name).list():
            yield key

class Feed(object):
    def __init__(self, user_id, feed_json_list):
        self.user_id = str(user_id)
        self.posts = []
        for post_json in feed_json_list:
            try:
                self.posts.append(FeedPost(post_json))
            except Exception:
                logger.debug("error parsing: " + str(post_json) + "\n\n")
                logger.deubg("full feed: " + str(feed_json_list) + "\n\n")
                raise

    def get_posts_str(self, delim="\t"):
        lines = []
        for p in self.posts:
            post_fields = [self.user_id, p.post_id, p.post_ts, p.post_type, p.post_app, p.post_from,
                      p.post_link, p.post_link_domain,
                      p.post_story, p.post_description, p.post_caption, p.post_message]
            post_line = delim.join(f.replace(delim, " ").encode('utf8', 'ignore') for f in post_fields)
            lines.append(post_line)
        return "\n".join(lines) + "\n"

    def get_links_str(self, delim="\t"):
        lines = []
        for p in self.posts:
            for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                has_to = "1" if user_id in p.to_ids else ""
                has_like = "1" if user_id in p.like_ids else ""
                has_comm = "1" if user_id in p.comment_ids else ""
                link_fields = [p.post_id, user_id, has_to, has_like, has_comm]
                link_line = "\t".join(f.encode('utf8', 'ignore') for f in link_fields)
                lines.append(link_line)
        return "\n".join(lines) + "\n"

    @staticmethod
    def write_labels(outfile_posts, outfile_links, delim="\t"):
        # these MUST match field order above
        post_fields = ['user_id', 'post_id', 'post_ts', 'post_type', 'post_app', 'post_from',
                       'post_link', 'post_link_domain',
                       'post_story', 'post_description', 'post_caption', 'post_message']
        outfile_posts.write(delim.join(post_fields) + "\n")

        link_fields = ['post_id', 'user_id', 'to', 'like', 'comment']
        outfile_links.write(delim.join(link_fields) + "\n")

class FeedS3(Feed):
    def __init__(self, key):
        # name should have format primary_secondary; e.g., "100000008531200_1000760833"
        prim_id, sec_id = map(int, key.name.split("_"))

        with tempfile.TemporaryFile() as fp:
            key.get_contents_to_file(fp)
            fp.seek(0)
            try:
                feed_json_list = json.load(fp)['data']
            except KeyError:
                logger.debug("no data in feed %s\n" % key.name)
                raise
        logger.debug("\tread feed json with %d posts from %s\n" % (len(feed_json_list), key.name))
        super(FeedS3, self).__init__(sec_id, feed_json_list)

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
        if 'to' in post_json:
            self.to_ids.update([user['id'] for user in post_json['to']['data']])
        if 'likes' in post_json:
            self.like_ids.update([user['id'] for user in post_json['likes']['data']])
        if 'comments' in post_json:
            self.comment_ids.update([user['id'] for user in post_json['comments']['data']])

def handle_feed(key):
    feed = FeedS3(key)
    post_lines = feed.get_posts_str()
    link_lines = feed.get_links_str()
    return post_lines, link_lines


class Timer(object):
    def __init__(self):
        self.start = time.time()
        self.ends = []
    def end(self):
        self.ends.append(time.time())
        return self.ends[-1] - ([self.start] + self.ends)[-2]
    def get_splits(self):
        splits = []
        s = self.start
        for e in self.ends:
            splits.append(e - s)
            s = e
        return splits
    def report_splits_avg(self, prefix=""):
        splits = self.get_splits()
        return prefix + "avg time over %d trials: %.1f secs" % (len(self.ends), sum(splits)/len(splits))



###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    parser.add_argument('post_file', type=str, help='out file for feed posts')
    parser.add_argument('link_file', type=str, help='out file for user-post links (like, comm)')
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--maxfeeds', type=int, help='bail after x feeds are done', default=None)
    parser.add_argument('--logfile', type=str, help='for debugging', default=None)
    parser.add_argument('--prof_trials', type=int, help='run x times with incr workers', default=1)
    parser.add_argument('--prof_incr', type=int, help='profile worker decrement', default=5)

    args = parser.parse_args()

    if args.logfile is not None:
        logging.basicConfig(filename=args.logfile, level=logging.DEBUG)

    outfile_posts = open(args.post_file, 'wb')
    outfile_links = open(args.link_file, 'wb')

    Feed.write_labels(outfile_posts, outfile_links)

    worker_counts = range(args.workers, 1, -1*args.prof_incr) + [1] if (args.prof_trials > 1) else [args.workers]
    sys.stderr.write("worker counts: %s\n" % str(worker_counts))

    for worker_count in worker_counts:
        tim = Timer()
        for t in range(args.prof_trials):
            sys.stderr.write("process %d farming out to %d childs\n" % (os.getpid(), worker_count))
            pool = multiprocessing.Pool(worker_count)
            for i, (post_lines, link_lines) in enumerate(pool.imap(handle_feed, key_iter())):
                if i % 100 == 0:
                    sys.stderr.write("\t%d\n" % i)
                outfile_posts.write(post_lines)
                outfile_links.write(link_lines)

                if (args.maxfeeds is not None) and (i >= args.maxfeeds):
                    #sys.exit("bailing")
                    break
            elapsed = tim.end()
            sys.stderr.write("\t%.1f elapsed\n" % elapsed)
        if (args.prof_trials > 1):
            sys.stderr.write(tim.report_splits_avg("%d workers " % worker_count) + "\n\n")
