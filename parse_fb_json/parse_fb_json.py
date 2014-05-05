#!/usr/bin/python
import sys
import os
import json
import logging
import psycopg2
from boto.s3.connection import S3Connection
from urlparse import urlparse
import tempfile
import argparse
import multiprocessing
from itertools import imap, repeat
import os.path
import time
from datetime import timedelta




AWS_ACCESS_KEY = "AKIAJDPO2KQRLOJBQP3Q"
AWS_SECRET_KEY = "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"
S3_BUCKET_NAMES = [ "user_feeds_%d" % i for i in range(5) ]
RS_HOST = 'wes-rs-inst.cd5t1q8wfrkk.us-east-1.redshift.amazonaws.com'
RS_USER = 'edgeflip'
RS_PASS = 'XzriGDp2FfVy9K'
RS_PORT = 5439
RS_DB = 'edgeflip'

DB_TEXT_LEN = 4096


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)

def key_iter(bucket_names=S3_BUCKET_NAMES):
    conn = get_conn_s3()
    for b, bucket_name in enumerate(bucket_names):
        logger.debug("reading bucket %d/%d (%s)" % (b, len(bucket_names), bucket_name))
        for key in conn.get_bucket(bucket_name).list():
            yield key


def get_conn_redshift(host=RS_HOST, user=RS_USER, password=RS_PASS, port=RS_PORT, db=RS_DB):
    logger.debug("connecting to Redshift" + " pid " + str(os.getpid()))
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, database=db)
    logger.debug("connect success" + " " + str(conn))
    return conn


def table_exists(curs, table_name):
    sql = "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)"
    curs.execute(sql, (table_name,))
    return curs.fetchone()[0]

def create_output_tables(conn):
    curs = conn.cursor()
    if table_exists(curs, 'posts'):  # DROP TABLE IF EXISTS is 8.2, Redshift says 8.0
        curs.execute("DROP TABLE posts;")
    sql = """
        CREATE TABLE posts (
          fbid_user BIGINT NOT NULL,
          fbid_post VARCHAR(64) NOT NULL,
          ts TIMESTAMP NOT NULL,
          type VARCHAR(64) NOT NULL,
          app VARCHAR(256),
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

    conn.commit()


def write_post_db(curs, fbid_user, fbid_post, ts, type,
               app=None, link=None, domain=None, story=None,
               description=None, caption=None, message=None):
    sql = """INSERT INTO posts (fbid_user, fbid_post, ts, type, app, link, domain,
                                story, description, caption, message)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    vals = (fbid_user, fbid_post, ts, type, app, link, domain, story, description, caption, message)
    curs.execute(sql, vals)




def write_link(curs, fbid_user, fbid_post, to, like, comment):
    sql = """INSERT INTO user_posts (fbid_user, fbid_post, user_to, user_like, user_comment)
             VALUES (%s, %s, %s, %s, %s)"""
    vals = (fbid_user, fbid_post, to, like, comment)
    curs.execute(sql, vals)


#
# def query_redshift_iter(sql, read_from_cache=False, write_to_cache=False, conn=None):
#     if (read_from_cache):
#         try:
#             cache_file = open(filename_from_sql(sql), 'r')
#         except IOError:
#             cache_file = None
#         if (cache_file is not None):
#             read_count = 0
#             print "reading from cache file"
#             #for line in cache_file:
#             #    yield line.rstrip("\n").split("\t")
#             while True:
#                 try:
#                     rec = pickle.load(cache_file)
#                 except EOFError:
#                     break
#                 read_count += 1
#                 #print "read rec %d: %s" % (read_count, str(rec))
#                 yield rec
#             print "got %d records, no more pickles" % read_count
#             return
#         else:
#             print "cache file not found"
#
#     if (conn is None):
#         conn = get_conn_redshift()
#     curs = conn.cursor()
#     print "querying db:\n\t" + sql
#     curs.execute(sql)
#     if (write_to_cache):
#         write_count = 0
#         outfile_fd, outfile_path = tempfile.mkstemp(suffix='.tsv', prefix='tmp')
#         outfile = os.fdopen(outfile_fd, 'w')
#         try:
#             for rec in curs:
#                 #outfile.write("\t".join([str(r) for r in rec]) + "\n")
#                 pickle.dump(rec, outfile)
#                 write_count += 1
#                 yield rec
#         except:
#             #os.close(outfile_fd)
#             outfile.close()
#             os.unlink(outfile_path)
#             raise
#         print "wrote %d records to cache file" % write_count
#         #os.close(outfile_fd)
#         outfile.close()
#         print "renaming " + outfile_path + " -> " + filename_from_sql(sql)
#         #os.rename(outfile_path, filename_from_sql(sql))
#         shutil.move(outfile_path, filename_from_sql(sql))
#     else:
#         for rec in curs:
#             yield rec






class FeedFromS3(object):
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
        logger.debug("\tread feed json with %d posts from %s" % (len(feed_json_list), key.name))

        self.user_id = fbid
        self.posts = []
        for post_json in feed_json_list:
            try:
                self.posts.append(FeedPostFromJson(post_json))
            except Exception:
                logger.debug("error parsing: " + str(post_json))
                # logger.debug("full feed: " + str(feed_json_list))
                raise

    def write_file(self, post_path, link_path, overwrite=False, delim="\t"):
        post_lines = []
        for p in self.posts:
            post_fields = [self.user_id, p.post_id, p.post_ts, p.post_type, p.post_app, p.post_from,
                           p.post_link, p.post_link_domain,
                           p.post_story, p.post_description, p.post_caption, p.post_message]
            post_lines.append(delim.join(f.replace(delim, " ").replace("\n", " ").encode('utf8', 'ignore') for f in post_fields))
        post_count = write_safe(post_path, post_lines, overwrite)

        link_lines = []
        for p in self.posts:
            for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                has_to = "1" if user_id in p.to_ids else ""
                has_like = "1" if user_id in p.like_ids else ""
                has_comm = "1" if user_id in p.comment_ids else ""
                link_fields = [p.post_id, user_id, has_to, has_like, has_comm]
                link_lines.append(delim.join(f.encode('utf8', 'ignore') for f in link_fields))
        link_count = write_safe(link_path, link_lines, overwrite)

        return (post_count, link_count)

    @staticmethod
    def write_file_labels(outfile_posts, outfile_links, delim="\t"):
        # these MUST match field order above
        post_fields = ['user_id', 'post_id', 'post_ts', 'post_type', 'post_app', 'post_from',
                       'post_link', 'post_link_domain',
                       'post_story', 'post_description', 'post_caption', 'post_message']
        outfile_posts.write_file(delim.join(post_fields) + "\n")

        link_fields = ['post_id', 'user_id', 'to', 'like', 'comment']
        outfile_links.write_file(delim.join(link_fields) + "\n")






    # def write_db(self, conn):
    #     curs = conn.cursor()
    #     post_count = 0
    #     for p in self.posts:
    #         try:
    #             write_post_db(curs, self.user_id, p.post_id, p.post_ts, p.post_type,
    #                           str(p.post_app), p.post_link, p.post_link_domain, p.post_story,
    #                           p.post_description, p.post_caption, p.post_message)
    #         except Exception as e:
    #             err = "error writing post record:\n"
    #             err += "\tuser:\t" + str(self.user_id) + "\n"
    #             err += "\tpost:\t" + str(p.post_id) + "\n"
    #             err += "\tts:\t" + str(p.post_ts) + "\n"
    #             err += "\ttype:\t" + str(p.post_type) + "\n"
    #             err += "\tapp:\t" + str(p.post_app) + "\n"
    #             err += "\tlink:\t" + str(p.post_link) + "\n"
    #             err += "\tdomain:\t" + str(p.post_link_domain) + "\n"
    #
    #             err += "\tstory (%d):\t %s\n" % (len(p.post_story), p.post_story)
    #             err += "\tdesc (%d):\t %s\n" % (len(p.post_description), p.post_description)
    #             err += "\tcaption (%d):\t %s\n" % (len(p.post_caption), p.post_caption)
    #             err += "\tmessage (%d):\t %s\n" % (len(p.post_message), p.post_message)
    #
    #             logger.error(err)
    #             raise
    #         post_count += 1
    #
    #     link_count = 0
    #     for p in self.posts:
    #         for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
    #             has_to = user_id in p.to_ids
    #             has_like = user_id in p.like_ids
    #             has_comm = user_id in p.comment_ids
    #
    #             try:
    #                 write_link(curs, user_id, p.post_id, has_to, has_like, has_comm)
    #             except Exception as e:
    #                 err = "error writing link record:\n"
    #                 err += "\tuser:\t" + str(user_id) + "\n"
    #                 err += "\tpost:\t" + str(p.post_id) + "\n"
    #                 err += "\tto:\t" + str(has_to) + "\n"
    #                 err += "\tlike:\t" + str(has_like) + "\n"
    #                 err += "\tcomment:\t" + str(has_comm) + "\n"
    #                 logger.error(err)
    #                 raise
    #             link_count += 1
    #     curs.close()
    #     conn.commit()
    #     return (post_count, link_count)





# def write_post_db(curs, fbid_user, fbid_post, ts, type,
#                app=None, link=None, domain=None, story=None,
#                description=None, caption=None, message=None):
#     sql = """INSERT INTO posts (fbid_user, fbid_post, ts, type, app, link, domain,
#                                 story, description, caption, message)
#              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#     vals = (fbid_user, fbid_post, ts, type, app, link, domain, story, description, caption, message)
#     curs.execute(sql, vals)

    def write_db(self, conn):
        curs = conn.cursor()

        post_val_tups = []
        link_val_tups = []
        for p in self.posts:
            post_vals = (self.user_id, p.post_id, p.post_ts, p.post_type,
                         str(p.post_app), p.post_link, p.post_link_domain,
                         p.post_story, p.post_description, p.post_caption, p.post_message)
            post_val_tups.append(post_vals)

            for user_id in p.to_ids.union(p.like_ids, p.comment_ids):
                has_to = user_id in p.to_ids
                has_like = user_id in p.like_ids
                has_comm = user_id in p.comment_ids
                link_vals = (user_id, p.post_id, has_to, has_like, has_comm)
                link_val_tups.append(link_vals)

        sql_posts = """INSERT INTO posts (fbid_user, fbid_post, ts, type, app, link, domain,
                                          story, description, caption, message)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        curs.executemany(sql_posts, post_val_tups)
        post_count = len(post_val_tups)

        sql_links = """INSERT INTO user_posts (fbid_user, fbid_post,
                                               user_to, user_like, user_comment)
                       VALUES (%s, %s, %s, %s, %s)"""
        curs.executemany(sql_links, link_val_tups)
        link_count = len(link_val_tups)

        curs.close()
        conn.commit()
        return (post_count, link_count)






class FeedPostFromJson(object):
    def __init__(self, post_json):
        self.post_id = str(post_json['id'])
        self.post_ts = post_json['updated_time']
        self.post_type = post_json['type']
        self.post_app = post_json['application']['id'] if 'application' in post_json else ""

        self.post_from = post_json['from']['id'] if 'from' in post_json else ""
        self.post_link = post_json.get('link', "")
        self.post_link_domain = urlparse(self.post_link).hostname if (self.post_link) else ""

        #todo: fix this terrible, terrible thing that limits the length of strings
        self.post_story = post_json.get('story', "")[:DB_TEXT_LEN / 2]
        self.post_description = post_json.get('description', "")[:DB_TEXT_LEN / 2]
        self.post_caption = post_json.get('caption', "")[:DB_TEXT_LEN / 2]
        self.post_message = post_json.get('message', "")[:DB_TEXT_LEN / 2]

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

def handle_feed_file(args):
    key, out_dir, overwrite = args

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    prim_id, sec_id = key.name.split("_")

    # For each primary (token owner), we create a directory, each secondary crawled with that
    # token is a file.  If the file already exists, we skip that feed.
    out_dir_prim = os.path.join(out_dir, prim_id)
    try:
        os.makedirs(out_dir_prim)
        logger.debug("created output sub directory: " + out_dir_prim)
    except OSError:
        logger.debug("output sub directory: " + out_dir_prim + " exists")
    out_file_path_posts, out_file_path_links = out_file_paths(out_dir, prim_id, sec_id)

    if (os.path.isfile(out_file_path_posts) or os.path.isfile(out_file_path_links)) and \
            (not overwrite):
        logging.debug("skipping existing prim %s, sec %s" % (prim_id, sec_id))
        return None
    else:
        try:
            feed = FeedFromS3(sec_id, key)
        except KeyError:  # gets logged and reraised upstream
            return None
        post_count, link_count = feed.write_file(out_file_path_posts, out_file_path_links, overwrite)
        return (post_count, link_count)

def handle_feed_db(args):
    key = args

    # pid__conn = {}
    #
    # pid = os.getpid()
    # logger.debug("pid " + str(pid) + " getting connection " + str(pid__conn))
    # if pid not in pid__conn:
    #     pid__conn[pid] = get_conn_redshift()
    #
    # conn = pid__conn[pid]


    pid = os.getpid()
    # logger.debug("pid " + str(pid) + " getting connection ")
    # conn = get_global_conn()
    logger.debug("pid " + str(pid) + ", key " + key.name + ", have conn: " + str(conn))

    # name should have format primary_secondary; e.g., "100000008531200_1000760833"
    prim_id, sec_id = key.name.split("_")

    logger.debug("pid " + str(pid) + " have prim, sec: " + prim_id + ", " + sec_id)

    try:

        logger.debug("pid " + str(pid) + " creating feed")
        feed = FeedFromS3(sec_id, key)
        logger.debug("pid " + str(pid) + " got feed")

    except KeyError:  # gets logged and reraised upstream
        logger.debug("pid " + str(pid) + " KeyError exception!")
        return None

    logger.debug("pid %d writing feed" % (pid))
    post_count, link_count = feed.write_db(conn)
    logger.debug("pid %d wrote %d posts, %d links" % (pid, post_count, link_count))

    return (post_count, link_count)

conn = None
def set_global_conn():
    global conn
    conn = get_conn_redshift()

# def process_feeds(out_dir, worker_count, max_feeds, overwrite):
def process_feeds(worker_count, max_feeds, overwrite):

    if overwrite:
        conn = get_conn_redshift()
        create_output_tables(conn)
        conn.close()

    logger.info("process %d farming out to %d childs" % (os.getpid(), worker_count))
    pool = multiprocessing.Pool(processes=worker_count, initializer=set_global_conn)

    post_line_count_tot = 0
    link_line_count_tot = 0
    # feed_arg_iter = imap(None, key_iter(), repeat(out_dir), repeat(overwrite))
    # feed_arg_iter = imap(None, key_iter())
    time_start = time.time()
    # for i, counts_tup in enumerate(pool.imap_unordered(handle_feed_file, feed_arg_iter)):
    for i, counts_tup in enumerate(pool.imap_unordered(handle_feed_db, key_iter())):
        if i % 1000 == 0:
            time_delt = timedelta(seconds=int(time.time()-time_start))
            logger.info("\t%s %d feeds, %d posts, %d links" % (str(time_delt), i, post_line_count_tot, link_line_count_tot))
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
    conn.close()
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
    logger.info("worker counts: %s" % str(worker_counts))
    for worker_count in worker_counts:
        tim = Timer()
        for t in range(profile_trials):
            process_feeds(out_dir, worker_count, max_feeds, overwrite)
            elapsed = tim.end()
        logger.info(tim.report_splits_avg("%d workers " % worker_count) + "\n\n")

def combine_output_files(out_dir, posts_path="posts.tsv", links_path="links.tsv"):
    post_file_count = 0
    link_file_count = 0
    with open(posts_path, 'wb') as outfile_posts, open(links_path, 'wb') as outfile_links:
        FeedFromS3.write_file_labels(outfile_posts, outfile_links)

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

def write_safe(outfile_path, lines, overwrite=False):
    #zzz this may leave temp files around if interrupted
    write_count = 0
    with tempfile.NamedTemporaryFile('wb', dir=os.path.dirname(outfile_path), delete=False) as temp:
        for line in lines:
            temp.write_file(line + "\n")
            write_count += 1
        if (not os.path.isfile(outfile_path)) or overwrite:
            os.rename(temp.name, outfile_path)
            return write_count
        else:
            raise Exception("cannot rename temp file")




###################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Eat up the FB sync data and put it into a tsv')
    # parser.add_argument('out_dir', type=str, help='base dir for output files')
    parser.add_argument('--workers', type=int, help='number of workers to multiprocess', default=1)
    parser.add_argument('--maxfeeds', type=int, help='bail after x feeds are done', default=None)
    parser.add_argument('--overwrite', action='store_true', help='overwrite previous runs')
    parser.add_argument('--logfile', type=str, help='for debugging', default=None)
    parser.add_argument('--prof_trials', type=int, help='run x times with incr workers', default=1)
    parser.add_argument('--prof_incr', type=int, help='profile worker decrement', default=5)
    parser.add_argument('--combine', action='store_true', help='create a single post and link file')
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
        # process_feeds(args.out_dir, args.workers, args.maxfeeds, args.overwrite)
        process_feeds(args.workers, args.maxfeeds, args.overwrite)

    else:
        profile_process_feeds(args.out_dir, args.workers, args.maxfeeds, args.overwrite,
                              args.prof_trials, args.prof_incr)

    if args.combine:
        combine_output_files(args.out_dir)

#zzz todo: do something more intelligent with \n and \t in text
