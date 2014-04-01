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
from itertools import imap, repeat
import os.path
import time




AWS_ACCESS_KEY = "AKIAJDPO2KQRLOJBQP3Q"
AWS_SECRET_KEY = "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"
# AWS_BUCKET_NAMES = [ "feed_crawler_%d" % i for i in range(5) ]
AWS_BUCKET_NAMES = [ "user_feeds_%d" % i for i in range(5) ]




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
        self.user_id = user_id
        self.posts = []
        for post_json in feed_json_list:
            try:
                self.posts.append(FeedPost(post_json))
            except Exception:
                logger.debug("error parsing: " + str(post_json) + "\n\n")
                logger.deubg("full feed: " + str(feed_json_list) + "\n\n")
                raise

    def write(self, path_posts, path_links, delim="\t"):
        count_posts = 0
        with open(path_posts, 'wb') as outfile_posts:
            for p in self.posts:
                post_fields = [self.user_id, p.post_id, p.post_ts, p.post_type, p.post_app, p.post_from,
                               p.post_link, p.post_link_domain,
                               p.post_story, p.post_description, p.post_caption, p.post_message]
                post_line = delim.join(f.replace(delim, " ").encode('utf8', 'ignore') for f in post_fields)
                outfile_posts.write(post_line + "\n")
                count_posts += 1

        count_links = 0
        with open(path_links, 'wb') as outfile_links:
            for p in self.posts:
                for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                    has_to = "1" if user_id in p.to_ids else ""
                    has_like = "1" if user_id in p.like_ids else ""
                    has_comm = "1" if user_id in p.comment_ids else ""
                    link_fields = [p.post_id, user_id, has_to, has_like, has_comm]
                    link_line = delim.join(f.encode('utf8', 'ignore') for f in link_fields)
                    outfile_links.write(link_line + "\n")
                    count_links += 1
        return (count_posts, count_links)

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
    def __init__(self, fbid, key):
        with tempfile.TemporaryFile() as fp:
            key.get_contents_to_file(fp)
            fp.seek(0)
            try:
                feed_json_list = json.load(fp)['data']
            except KeyError:
                logger.debug("no data in feed %s\n" % key.name)
                raise
        logger.debug("\tread feed json with %d posts from %s\n" % (len(feed_json_list), key.name))
        super(FeedS3, self).__init__(fbid, feed_json_list)

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

def out_file_paths(out_dir, prim_id, sec_id):
    out_file_path_posts = os.path.join(out_dir, prim_id, sec_id + "_posts.tsv")
    out_file_path_links = os.path.join(out_dir, prim_id, sec_id + "_links.tsv")
    return (out_file_path_posts, out_file_path_links)

def handle_feed(args):
    key, out_dir, overwrite = args

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    prim_id, sec_id = key.name.split("_")

    # For each primary (token owner), we create a directory, each secondary crawled with that
    # token is a file.  If the file already exists, we skip that feed.
    out_dir_prim = os.path.join(out_dir, prim_id)
    if not os.path.exists(out_dir_prim):
        os.makedirs(out_dir_prim)
    out_file_path_posts, out_file_path_links = out_file_paths(out_dir, prim_id, sec_id)

    if (os.path.isfile(out_file_path_posts) or os.path.isfile(out_file_path_links)) and \
            (not overwrite):
        logging.debug("skipping existing prim %s, sec %s" % (prim_id, sec_id))
        return None
    else:
        try:
            feed = FeedS3(sec_id, key)
        except KeyError:  # gets logged and reraised upstream
            return None
        post_line_count, link_line_count = feed.write(out_file_path_posts, out_file_path_links)
        return (post_line_count, link_line_count)

def process_feeds(out_dir, worker_count, max_feeds, overwrite):
    sys.stderr.write("process %d farming out to %d childs\n" % (os.getpid(), worker_count))
    pool = multiprocessing.Pool(worker_count)

    post_line_count_tot = 0
    link_line_count_tot = 0
    feed_arg_iter = imap(None, key_iter(), repeat(out_dir), repeat(overwrite))
    for i, counts_tup in enumerate(pool.imap_unordered(handle_feed, feed_arg_iter)):
        if i % 100 == 0:
            sys.stderr.write("\t%d feeds, %d posts, %d links\n" % (i, post_line_count_tot, link_line_count_tot))
        if counts_tup is None:
            continue
        else:
            post_lines, link_lines = counts_tup
            post_line_count_tot += post_lines
            link_line_count_tot += link_lines

        if (max_feeds is not None) and (i >= max_feeds):
            #sys.exit("bailing")
            break
    pool.terminate()
    return i

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

def profile_process_feeds(out_dir, max_worker_count, max_feeds, overwrite,
                          profile_trials, profile_incr):
    worker_counts = range(max_worker_count, 1, -1*profile_incr) + [1]
    sys.stderr.write("worker counts: %s\n" % str(worker_counts))
    for worker_count in worker_counts:
        tim = Timer()
        for t in range(profile_trials):
            process_feeds(worker_count, max_feeds, out_dir, overwrite)
            elapsed = tim.end()
        sys.stderr.write(tim.report_splits_avg("%d workers " % worker_count) + "\n\n")

def combine_output_files(out_dir, posts_path="posts.tsv", links_path="links.tsv"):
    post_file_count = 0
    link_file_count = 0
    with open(posts_path, 'wb') as outfile_posts, open(links_path, 'wb') as outfile_links:
        Feed.write_labels(outfile_posts, outfile_links)

        for dirpath, dirnames, filenames in os.walk(out_dir):
            for filename in filenames:
                logging.debug("combining " + os.path.join(dirpath, filename))
                if filename.endswith("_posts.tsv"):  # must match format in out_file_paths()
                    outfile = outfile_posts
                    post_file_count += 1
                elif filename.endswith("_links.tsv"):  # must match format in out_file_paths()
                    outfile = outfile_links
                    link_file_count += 1
                else:
                    continue
                with open(os.path.join(dirpath, filename)) as infile:
                    outfile.write(infile.read())
    logging.debug("combined %d post files, %d link files" % (post_file_count, link_file_count))





###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    parser.add_argument('out_dir', type=str, help='base dir for output files')
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--maxfeeds', type=int, help='bail after x feeds are done', default=None)
    parser.add_argument('--overwrite', action='store_true', help='overwrite previous runs')
    parser.add_argument('--logfile', type=str, help='for debugging', default=None)
    parser.add_argument('--prof_trials', type=int, help='run x times with incr workers', default=1)
    parser.add_argument('--prof_incr', type=int, help='profile worker decrement', default=5)
    parser.add_argument('--combine', action='store_true', help='create a single post and link file')
    args = parser.parse_args()

    if args.logfile is not None:
        logging.basicConfig(filename=args.logfile, level=logging.DEBUG)

    if (args.prof_trials == 1):
        process_feeds(args.out_dir, args.workers, args.maxfeeds, args.overwrite)
    else:
        profile_process_feeds(args.out_dir, args.workers, args.maxfeeds, args.overwrite,
                              args.prof_trials, args.prof_incr)

    if args.combine:
        combine_output_files(args.out_dir)


#zzz todo: write to temp files first then rename
