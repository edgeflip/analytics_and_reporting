from collections import namedtuple
from cStringIO import StringIO
import random
import time
import sys
import os
import re

from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import csr_matrix, spdiags
from scipy.io import mmwrite, mmread
import joblib

from db_utils import redshift_connect, redshift_disconnect
from db_utils import execute_query, does_table_exist

User_Posts = namedtuple('User_Posts', ['fbid', 'posts', 'aboutme'])
MAX_DF = 0.25
MIN_DF = 0.01
NGRAM_RANGE = (1, 2)
TFIDF_TRAINING_SAMPLE_SIZE = 20000

def get_ids_from_file(ids_filename):
    if os.path.isfile(ids_filename):
        ids_file = open(ids_filename, 'r')
        ids = [line.strip() for line in ids_file if line.strip()]
        ids_file.close()
        return ids
    else:
        return []

def get_post_vectors(tfidf_vectorizer, user_posts_list, post_vector_matrix_filename, 
                     report_file=sys.stdout, cache=True):
    if cache and os.path.isfile(post_vector_matrix_filename):
        report_file.write('\tReading in cached post vectors\n')
        return csr_matrix(mmread(post_vector_matrix_filename))
    else:
        vector_mx = tfidf_vectorizer.transform(
                                [user_posts.posts for user_posts in user_posts_list])
        if cache:
            mmwrite(post_vector_matrix_filename, vector_mx)
        return vector_mx

def get_user_posts(post_document_filename, aboutme_document_filename, *user_sets):
    '''
    Return a list of lists of User_Posts by scanning all posts in post_document_filename
    and recording them according to the position of the user_set that the user falls 
    into (if at all). If no user_sets are supplied, it returns a list of a single 
    list of User_Posts.
    '''
    if not user_sets:
        user_to_posts = [{}]
    else:
        user_to_posts = [{} for i in range(len(user_sets))]
    
    if post_document_filename:
        post_document_file = open(post_document_filename, 'r')
        for line in post_document_file:
            vals = line.split()
            user = vals[0].split('_')[0]
            if not user_sets:
                bucket = 0
            else:
                bucket = -1
                for idx, user_set in enumerate(user_sets):
                    if user in user_set:
                        bucket = idx
                        break
            if bucket != -1:
                words = ' '.join(vals[1:])
                user_to_posts[bucket].setdefault(user, [StringIO(), StringIO()])
                user_to_posts[bucket][user][0].write(words)
                user_to_posts[bucket][user][0].write(' ')
        post_document_file.close()
    
    if aboutme_document_filename:
        aboutme_document_file = open(aboutme_document_filename, 'r')
        for line in aboutme_document_file:
            vals = line.split()
            user = vals[0].split('_')[0]
            if not user_sets:
                bucket = 0
            else:
                bucket = -1
                for idx, user_set in enumerate(user_sets):
                    if user in user_set:
                        bucket = idx
                        break
            if bucket != -1:
                words = ' '.join(vals[1:])
                user_to_posts[bucket].setdefault(user, [StringIO(), StringIO()])
                user_to_posts[bucket][user][1].write(words)
                user_to_posts[bucket][user][1].write(' ')
        aboutme_document_file.close()
    
    return [[User_Posts(user, posts.getvalue(), aboutme.getvalue()) 
                for user, (posts, aboutme) in user_to_posts[bucket].items()] 
                for bucket in range(len(user_to_posts))]

def get_post_and_aboutme_document_from_id(fbid_in, conn):
    # Get post words.
    query = """
            SELECT fbid_post, MIN(message) as message
            FROM posts_raw
            WHERE fbid_user = {fbid}
                  AND message <> ''
                  AND fbid_user = post_from
            GROUP BY fbid_post
            """.format(fbid=fbid_in)
    sys.stdout.write('\tgetting post data')
    sys.stdout.flush()
    t0 = time.time()
    rows = execute_query(query, conn)
    sys.stdout.write('\t{}\n'.format(time.time() - t0))
    sys.stdout.flush()
    
    post_message = StringIO()
    for row in rows:
        post_message.write(' '.join(re.split('\s+', row[1])))
        post_message.write(' ')
    
    # Get aboutme words.
    query = """
            SELECT u.fbid, books, interests, movies, tv, music, quotes, sports
            FROM users u
            WHERE u.fbid = {fbid}
            """.format(fbid=fbid_in)
    sys.stdout.write('\tgetting aboutme data')
    sys.stdout.flush()
    t0 = time.time()
    row =  execute_query(query, conn)[0]
    sys.stdout.write('\t{}\n'.format(time.time() - t0))
    sys.stdout.flush()
    books, interests, movies, tv, music, quotes, sports = row[1:]
    if not books:
        books = ''
    if not interests:
        interests = ''
    if not movies:
        movies = ''
    if not tv:
        tv = ''
    if not music:
        music = ''
    if not quotes:
        quotes = ''
    if not sports:
        sports = ''        
    aboutme_message = ' '.join([books, interests, movies, tv, 
                                music, quotes, sports]).strip()
    if aboutme_message:
        aboutme_message = ' '.join(re.split('\s+', aboutme_message))
            
    return User_Posts(fbid_in, post_message.getvalue().strip(), aboutme_message.strip())

def order_user_posts(user_posts, user_ids):
    '''
    Sort a list of User_Posts by the fixed order occurring in user_ids
    '''
    user_id_to_user_post = {}
    for user_post in user_posts:
        user_id_to_user_post[user_post.fbid] = user_post
    return [user_id_to_user_post[user_id] for user_id in user_ids 
                                            if user_id in user_id_to_user_post]

def prune_ids_and_posts_by_thresholds(ids, user_posts, post_threshold, 
                                      aboutme_threshold, label, report_file=sys.stdout):
    temp_ids_pruned = []
    temp_user_posts_pruned = []
    post_threshold = 0 if not post_threshold else post_threshold
    aboutme_threshold = 0 if not aboutme_threshold else aboutme_threshold
    for user_id, user_post in zip(ids, user_posts):
        if (len(user_post.posts) >= post_threshold or 
            len(user_post.aboutme) >= aboutme_threshold):
            # retain the user because it matched one of the thresholds
            temp_ids_pruned.append(user_id)
            temp_user_posts_pruned.append(user_post)
    report_file.write('\tremoved {} {}\n'.format(len(ids) - len(temp_ids_pruned), label))
    ids = temp_ids_pruned[:]
    user_posts = temp_user_posts_pruned[:]
    return ids, user_posts

def get_vectorizer(vocabulary_filename, idf_filename, user_posts_list, 
                   negative_train_vector_matrix_filename, 
                   report_file=sys.stdout, bad_phrases_filename=None):
    if os.path.isfile(vocabulary_filename) and os.path.isfile(idf_filename):
        report_file.write('\tReading in cached tfidf vectorizer\n')
        report_file.flush()
        return load_vectorizer_from_files(vocabulary_filename, idf_filename)
    elif os.path.isfile(vocabulary_filename) and not os.path.isfile(idf_filename):
        vocab = joblib.load(vocabulary_filename)
        if bad_phrases_filename is not None and os.path.isfile(bad_phrases_filename):
            bad_phrases_file = open(bad_phrases_filename, 'r')
            bad_phrases = {line.strip() for line in bad_phrases_file}
            vocab = [v for v in vocab.keys() if v not in bad_phrases]
        return train_vectorizer(vocabulary_filename, idf_filename, 
                                user_posts_list, vocabulary=vocab)
    else:
        # No vectorizer cached, fit a new one to the given user-posts 
        return train_vectorizer(vocabulary_filename, idf_filename, user_posts_list)

def train_vectorizer(vocabulary_filename, idf_filename, user_posts_list, vocabulary=None):
    tfidf_vectorizer = TfidfVectorizer(ngram_range=NGRAM_RANGE, max_df=MAX_DF, 
                                       min_df=MIN_DF, vocabulary=vocabulary)
    training_posts = [user_posts.posts for user_posts in user_posts_list]
    random.shuffle(training_posts)
    tfidf_vectorizer.fit(training_posts[:TFIDF_TRAINING_SAMPLE_SIZE])
    joblib.dump(tfidf_vectorizer.vocabulary_, vocabulary_filename)
    joblib.dump(tfidf_vectorizer.idf_, idf_filename)
    
    # Reload vectorizer to save memory (drops its stop-word list and uses given vocab)
    tfidf_vectorizer = load_vectorizer_from_files(vocabulary_filename, idf_filename)
    return tfidf_vectorizer

def load_vectorizer_from_files(vocabulary_filename, idf_filename):
    vocab = joblib.load(vocabulary_filename)
    idf = joblib.load(idf_filename)
    tfidf_vectorizer = TfidfVectorizer(ngram_range=NGRAM_RANGE, max_df=MAX_DF, 
                                       min_df=MIN_DF, vocabulary=vocab)
    n_features = idf.shape[0]
    tfidf_vectorizer._tfidf._idf_diag = spdiags(idf, diags=0, 
                                                m=n_features, n=n_features)
    return tfidf_vectorizer

def get_names_from_fbids(fbids):
    conn = redshift_connect()
    temp_table_name = 'temp_fbid_list'
    if does_table_exist(temp_table_name, conn):
        query = 'DROP TABLE {}'.format(temp_table_name)
        execute_query(query, conn, fetchable=False)
    query = 'CREATE TEMP TABLE {} (fbid bigint)'.format(temp_table_name)
    execute_query(query, conn, fetchable=False)
    
    query = StringIO()
    query.write('INSERT INTO {} VALUES '.format(temp_table_name))
    for fbid in fbids:
        query.write('({}),'.format(fbid))
    query = query.getvalue()[:-1]
    execute_query(query, conn, fetchable=False)

    query = """
            SELECT fbid, fname, lname
            FROM users
                JOIN {} USING (fbid)
            """.format(temp_table_name)
    rows = execute_query(query, conn)    
    fbid_to_name = {}
    for row in rows:
        fname = '' if not row[1] else row[1]
        lname = '' if not row[2] else row[2]
        fbid_to_name[str(row[0])] = fname + ' ' + lname
    return fbid_to_name
