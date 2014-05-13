#!/usr/bin/python
import sys
import os
import json
import logging
import psycopg2
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from urlparse import urlparse
import tempfile
import argparse
import multiprocessing
from itertools import imap, repeat
import os.path
import time
import datetime


AWS_ACCESS_KEY = "AKIAJDPO2KQRLOJBQP3Q"
AWS_SECRET_KEY = "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"
S3_IN_BUCKET_NAMES = [ "user_feeds_%d" % i for i in range(5) ]
S3_OUT_BUCKET_NAME = "user_feeds_parsed"
S3_DONE_DIR = "loaded"
RS_HOST = 'wes-rs-inst.cd5t1q8wfrkk.us-east-1.redshift.amazonaws.com'
RS_USER = 'edgeflip'
RS_PASS = 'XzriGDp2FfVy9K'
RS_PORT = 5439
RS_DB = 'edgeflip'

DB_TEXT_LEN = 4096


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


# S3 stuff

def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)

def s3_key_iter(bucket_names=S3_IN_BUCKET_NAMES):
    conn_s3 = get_conn_s3()
    for b, bucket_name in enumerate(bucket_names):
        logger.debug("reading bucket %d/%d (%s)" % (b, len(bucket_names), bucket_name))
        for key in conn_s3.get_bucket(bucket_name).list():
            yield key
    conn_s3.close()

def delete_s3_bucket(conn_s3, bucket_name):
    buck = conn_s3.get_bucket(bucket_name)
    for key in buck.list():
        key.delete()
    conn_s3.delete_bucket(bucket_name)

def create_s3_bucket(conn_s3, bucket_name):
    if conn_s3.lookup(bucket_name) is not None:
        logger.debug("deleting old S3 bucket " + bucket_name)
        delete_s3_bucket(conn_s3, bucket_name)
    logger.debug("creating S3 bucket " + bucket_name)
    return conn_s3.create_bucket(bucket_name)


# Redshift stuff

def get_conn_redshift(host=RS_HOST, user=RS_USER, password=RS_PASS, port=RS_PORT, db=RS_DB):
    logger.debug("connecting to Redshift" + " pid " + str(os.getpid()))
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, database=db)
    logger.debug("connect success" + " " + str(conn))
    return conn

def table_exists(curs, table_name):
    sql = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
    curs.execute(sql, (table_name,))
    return curs.fetchone()[0]

def create_output_tables(conn_rs):
    curs = conn_rs.cursor()
    if table_exists(curs, 'posts'):  # DROP TABLE IF EXISTS is Postgres 8.2, Redshift is 8.0
        curs.execute("DROP TABLE posts;")
    sql = """
        CREATE TABLE posts (
          fbid_user BIGINT NOT NULL,
          fbid_post VARCHAR(64) NOT NULL,
          ts TIMESTAMP NOT NULL,
          type VARCHAR(64) NOT NULL,
          app VARCHAR(256),
          post_from VARCHAR(256),
          link VARCHAR(2048),
          domain VARCHAR(1024),
          story VARCHAR(%s),
          description VARCHAR(%s),
          caption VARCHAR(%s),
          message VARCHAR(%s)
        );
    """
    logger.debug("creating posts table: " + " ".join(sql.split()))
    ret = curs.execute(sql, (DB_TEXT_LEN, DB_TEXT_LEN, DB_TEXT_LEN, DB_TEXT_LEN))
    logging.info("created posts table: " + str(ret))

    if table_exists(curs, 'user_posts'):
        curs.execute("DROP TABLE user_posts;")
    sql = """
        CREATE TABLE user_posts (
          fbid_user VARCHAR(64) NOT NULL,
          fbid_post VARCHAR(64) NOT NULL,
          user_to BOOLEAN,
          user_like BOOLEAN,
          user_comment BOOLEAN
        );
    """
    logger.debug("creating links table: " + " ".join(sql.split()))
    ret = curs.execute(sql)
    logging.info("created user_posts table: " + str(ret))

    conn_rs.commit()

def load_db_from_s3(conn_rs, conn_s3, bucket_name, key_names, table_name, dest_dir, delim="\t"):
    buck = conn_s3.get_bucket(bucket_name)
    curs = conn_rs.cursor()
    for i, key_name in enumerate(key_names):
        logger.debug("pid %s loading %d/%d %s into %s" % (str(os.getpid()), i, len(key_names), key_name, table_name))
        sql = "COPY %s FROM 's3://%s/%s' " % (table_name, bucket_name, key_name)
        sql += "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' " % (AWS_ACCESS_KEY, AWS_SECRET_KEY)
        sql += "DELIMITER '%s' TRUNCATECOLUMNS ACCEPTINVCHARS NULL AS '\000'" % delim
        # logger.debug(sql)
        try:
            curs.execute(sql)
        except psycopg2.InternalError as e:
            logger.debug("error loading: \n" + get_load_errs())
            raise

        conn_rs.commit()

        logger.debug("moving key %s to %s" % (key_name, dest_dir))
        buck.copy_key(os.path.join(dest_dir, key_name), bucket_name, key_name)
        buck.delete_key(key_name)
        logger.debug("done moving key %s to %s" % (key_name, dest_dir))

# useful for debugging
def get_load_errs():
    # see: http://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html
    conn_rs = get_conn_redshift()
    curs = conn_rs.cursor()
    sql = "select * from stl_load_errors order by starttime desc limit 2"
    curs.execute(sql)
    fmt = ": %s\n\t".join(["userid", "slice", "tbl", "starttime", "session", "query", "filename",
                           "line_number", "colname", "type", "col_length", "position", "raw_line",
                           "raw_field_value", "err_code", "err_reason"]) + ": %s\n"
    ret = ""
    for row in curs.fetchall():
        ret += fmt % tuple([str(r)[:80] for r in row])
    return ret


# data structs for transforming json to db rows

class FeedFromS3(object):
    """Holds an entire feed from a single user crawl"""

    def __init__(self, fbid, key):
        with tempfile.TemporaryFile() as fp:
            key.get_contents_to_file(fp)
            fp.seek(0)
            feed_json = json.load(fp)
            try:
                feed_json_list = feed_json['data']
            except KeyError:
                logger.debug("no data in feed %s" % key.name)
                logger.debug(str(feed_json))
                raise
        logger.debug("%s got feed with %d posts from %s" % (os.getpid(), len(feed_json_list), key.name))

        self.user_id = fbid
        self.posts = []
        for post_json in feed_json_list:
            try:
                self.posts.append(FeedPostFromJson(post_json))
            except Exception:
                logger.debug("error parsing: " + str(post_json))
                # logger.debug("full feed: " + str(feed_json_list))
                raise

    def get_post_lines(self, delim="\t"):
        post_lines = []
        for p in self.posts:
            post_fields = [self.user_id, p.post_id, p.post_ts, p.post_type, p.post_app, p.post_from,
                           p.post_link, p.post_link_domain,
                           p.post_story, p.post_description, p.post_caption, p.post_message]
            line = delim.join(f.replace(delim, " ").replace("\n", " ").encode('utf8', 'ignore') for f in post_fields)
            post_lines.append(line)
        return post_lines

    def get_link_lines(self, delim="\t"):
        link_lines = []
        for p in self.posts:
            for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                has_to = "1" if user_id in p.to_ids else ""
                has_like = "1" if user_id in p.like_ids else ""
                has_comm = "1" if user_id in p.comment_ids else ""
                link_fields = [p.post_id, user_id, has_to, has_like, has_comm]
                link_lines.append(delim.join(f.encode('utf8', 'ignore') for f in link_fields))
        return link_lines

    def write_s3(self, conn_s3, bucket_name, key_name_posts, key_name_links, delim="\t"):
        buck = conn_s3.get_bucket(bucket_name)

        post_lines = self.get_post_lines()
        key_posts = Key(buck)
        key_posts.key = key_name_posts
        key_posts.set_contents_from_string("\n".join(post_lines))

        link_lines = self.get_link_lines()
        key_links = Key(buck)
        key_links.key = key_name_links
        key_links.set_contents_from_string("\n".join(link_lines))

        return len(post_lines), len(link_lines)


# Despite what the docs say, datetime.strptime() format doesn't like %z
# see: http://stackoverflow.com/questions/526406/python-time-to-age-part-2-timezones/526450#526450
def parse_ts(time_string):
    tz_offset_hours = int(time_string[-5:]) / 100  # we're ignoring the possibility of minutes here
    tz_delt = datetime.timedelta(hours=tz_offset_hours)
    return datetime.datetime.strptime(time_string[:-5], "%Y-%m-%dT%H:%M:%S") - tz_delt

class FeedPostFromJson(object):
    """Each post contributes a single post line, and multiple user-post lines to the db"""

    def __init__(self, post_json):
        self.post_id = str(post_json['id'])
        # self.post_ts = post_json['updated_time']
        self.post_ts = parse_ts(post_json['updated_time']).strftime("%Y-%m-%d %H:%M:%S")
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

# Each worker gets its own S3 connection, manage that with a global variable.  There's prob
# a better way to do this.
# see: http://stackoverflow.com/questions/10117073/how-to-use-initializer-to-set-up-my-multiprocess-pool
conn_s3_global = None
def set_global_conns():
    global conn_s3_global
    conn_s3_global = get_conn_s3()

def handle_feed_s3(args):
    key, bucket_name = args  #zzz todo: there's got to be a better way to handle this

    pid = os.getpid()
    logger.debug("pid " + str(pid) + ", key " + key.name)

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    prim_id, sec_id = key.name.split("_")
    # logger.debug("pid " + str(pid) + " have prim, sec: " + prim_id + ", " + sec_id)

    try:
        # logger.debug("pid " + str(pid) + " creating feed")
        feed = FeedFromS3(sec_id, key)
        # logger.debug("pid " + str(pid) + " got feed")
    except KeyError:  # gets logged and reraised upstream
        logger.debug("pid " + str(pid) + " KeyError exception!")
        return None

    key_name_posts = str(sec_id) + "_posts.tsv"
    key_name_links = str(sec_id) + "_links.tsv"
    counts = feed.write_s3(conn_s3_global, bucket_name, key_name_posts, key_name_links)

    return (key_name_posts, key_name_links)


def process_feeds(worker_count, max_feeds, overwrite, load_thresh, bucket_name):

    conn_rs = get_conn_redshift()  # keep these connections around
    conn_s3 = get_conn_s3()
    if (overwrite):
        create_output_tables(conn_rs)
        create_s3_bucket(conn_s3, bucket_name)

    logger.info("process %d farming out to %d childs" % (os.getpid(), worker_count))
    pool = multiprocessing.Pool(processes=worker_count, initializer=set_global_conns)

    post_file_names = []
    link_file_names = []
    feed_arg_iter = imap(None, s3_key_iter(), repeat(bucket_name))
    time_start = time.time()
    # for i, out_file_names in enumerate(pool.imap_unordered(handle_feed_s3, feed_arg_iter)):
    for i, out_file_names in enumerate(pool.imap(handle_feed_s3, feed_arg_iter)):

        if i % 1000 == 0:
            time_delt = datetime.timedelta(seconds=int(time.time()-time_start))
            logger.info("\t%s %d feeds, %d posts, %d links" % (str(time_delt), i, len(post_file_names), len(link_file_names)))

        if out_file_names is None:  # error reading the key
            continue
        else:
            post_file_name, link_file_name = out_file_names
            post_file_names.append(post_file_name)
            link_file_names.append(link_file_name)

        if (max_feeds is not None) and (i >= max_feeds):
            #sys.exit("bailing")
            break

        #todo: this should probably be spun off into another process so it doesn't hold things up
        if max(len(post_file_names), len(link_file_names)) >= load_thresh:
            logger.info("%d/%d feeds processed, loading %d posts, %d links into db" % (i, load_thresh, len(post_file_names), len(link_file_names)))
            load_db_from_s3(conn_rs, conn_s3, bucket_name, post_file_names, "posts", S3_DONE_DIR)
            logger.info("loaded %d post files" % (len(post_file_names)))
            load_db_from_s3(conn_rs, conn_s3, bucket_name, link_file_names, "user_posts", S3_DONE_DIR)
            logger.info("loaded %d link files" % (len(link_file_names)))

            post_file_names = []
            link_file_names = []
        else:
            logger.debug("%d/%d users processed, delaying load" % (i, load_thresh))

    # load whatever is left over
    load_db_from_s3(conn_rs, conn_s3, bucket_name, post_file_names, "posts", S3_DONE_DIR)
    load_db_from_s3(conn_rs, conn_s3, bucket_name, link_file_names, "user_posts", S3_DONE_DIR)

    pool.terminate()
    conn_s3.close()
    conn_rs.close()
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

def profile_process_feeds(max_worker_count, max_feeds, overwrite, load_thresh, bucket_name,
                          profile_trials, profile_incr):
    worker_counts = range(max_worker_count, 1, -1*profile_incr) + [1]
    logger.info("worker counts: %s" % str(worker_counts))
    for worker_count in worker_counts:
        tim = Timer()
        for t in range(profile_trials):
            process_feeds(worker_count, max_feeds, overwrite, load_thresh, bucket_name)

            elapsed = tim.end()
        logger.info(tim.report_splits_avg("%d workers " % worker_count) + "\n\n")




###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync json and put it into Redshift')
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--maxfeeds', type=int, help='bail after x feeds are done', default=None)
    parser.add_argument('--overwrite', action='store_true', help='overwrite previous runs')
    parser.add_argument('--logfile', type=str, help='for debugging', default=None)
    parser.add_argument('--loadthresh', type=int, default=100,
                        help='number of feeds to write to file before loading to db')
    parser.add_argument('--bucket', type=str, default=S3_OUT_BUCKET_NAME,
                        help='S3 bucket for writing transformed data and loading into Redshift')
    parser.add_argument('--prof_trials', type=int, help='run x times with incr workers', default=1)
    parser.add_argument('--prof_incr', type=int, help='profile worker decrement', default=5)
    args = parser.parse_args()

    hand_s = logging.StreamHandler()
    hand_s.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    if args.logfile is None:
        hand_s.setLevel(logging.DEBUG)
    else:
        hand_s.setLevel(logging.INFO)
        hand_f = logging.FileHandler(args.logfile)
        hand_f.setFormatter(logging.Formatter('%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'))
        hand_f.setLevel(logging.DEBUG)
        logger.addHandler(hand_f)
    logger.addHandler(hand_s)

    if args.prof_trials == 1:
        process_feeds(args.workers, args.maxfeeds, args.overwrite, args.loadthresh, args.bucket)

    else:
        profile_process_feeds(args.workers, args.maxfeeds, args.overwrite,
                              args.loadthresh, args.bucket,
                              args.prof_trials, args.prof_incr)



#zzz todo: do something more intelligent with \n and \t in text

#zzz todo: audit (non-)use of delim for different handlers

