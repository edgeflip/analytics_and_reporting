from cStringIO import StringIO
import random
import time
import sys
import re

import pandas as pd
import joblib

from db_utils import redshift_connect, redshift_disconnect
from db_utils import execute_query, does_table_exist
from predict_utils import get_post_vectors, load_vectorizer_from_files
from predict_utils import User_Posts

def get_user_document_from_ids(fbids, conn):
    batch_table_name = 'batch_ids_{}'.format(random.randint(0, 1000000))
    if does_table_exist(batch_table_name, conn):
        query = 'DROP TABLE {}'.format(batch_table_name)
        execute_query(query, conn, fetchable=False)
    query = 'CREATE TEMP TABLE {} (fbid bigint)'.format(batch_table_name)
    execute_query(query, conn, fetchable=False)
    query = StringIO()
    query.write('INSERT INTO {} VALUES '.format(batch_table_name))
    for fbid in fbids:
        query.write('({}),'.format(fbid))
    query = query.getvalue()[:-1]
    execute_query(query, conn, fetchable=False)
    return get_post_and_aboutme_document_from_ids_helper(batch_table_name, conn)

def get_post_and_aboutme_document_from_ids_helper(batch_table_name, conn):
    fbid_to_message = {}
    # Get post words.
    query = """
            SELECT fbid_post, MIN(message) as message
            FROM posts_raw p
                JOIN {batch_table_name} b
                 ON p.fbid_user = b.fbid
            WHERE message <> ''
                  AND fbid_user = post_from
            GROUP BY fbid_post
            """.format(batch_table_name=batch_table_name)
    sys.stdout.write('\tgetting post data')
    sys.stdout.flush()
    t0 = time.time()
    rows = execute_query(query, conn)
    sys.stdout.write('\t{}\n'.format(time.time() - t0))
    sys.stdout.flush()

    for row in rows:
        fbid = int(row[0].split('_')[0])
        fbid_to_message.setdefault(fbid, StringIO())
        fbid_to_message[fbid].write(' '.join(re.split('\s+', row[1])))
        fbid_to_message[fbid].write(' ')
        
    # Get aboutme words.
    query = """
            SELECT u.fbid, books, interests, movies, tv, music, quotes, sports
            FROM users u
                JOIN {batch_table_name} b
                  ON u.fbid = b.fbid
            """.format(batch_table_name=batch_table_name)
    sys.stdout.write('\tgetting aboutme data')
    sys.stdout.flush()
    t0 = time.time()
    rows =  execute_query(query, conn)
    sys.stdout.write('\t{}\n'.format(time.time() - t0))
    sys.stdout.flush()    
    for row in rows:
        fbid = int(row[0])
        fbid_to_message.setdefault(fbid, StringIO())
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
        aboutme = ' '.join([books, interests, movies, tv, music, quotes, sports]).strip()
        if aboutme:
            aboutme = ' '.join(re.split('\s+', aboutme))
            fbid_to_message[fbid].write(' ')
            fbid_to_message[fbid].write(aboutme)
    
    # create user_posts list, segment out fbids with no messages and align the lists
    fbids_with_no_words = []
    fbids_with_words = []
    user_posts_list = []
    for fbid, message in fbid_to_message.items():
        words = message.getvalue().strip()
        if not words:
            fbids_with_no_words.append(fbid)
        else:
            fbids_with_words.append(fbid)
            user_posts_list.append(User_Posts(fbid, words, ''))

    return fbids_with_words, user_posts_list, fbids_with_no_words

def create_prediction_table(prediction_table_name, conn):
    if not does_table_exist(prediction_table_name, conn):        
        query = """
                CREATE TABLE {}
                (fbid bigint not null, prediction double precision)
                distkey (fbid)
                sortkey (fbid)
                """.format(prediction_table_name)
        execute_query(query, conn, fetchable=False)

def insert_predictions(predictions_df, prediction_table_name, conn):
    query = StringIO()
    query.write('INSERT INTO {} VALUES'.format(prediction_table_name))
    ctr = 0
    for row in predictions_df.iterrows():
        ctr += 1
        query.write('\n({},{}),'.format(int(row[1]['id']), row[1]['decision']))
    if ctr > 0: # Only insert if there's something to insert
        execute_query(query.getvalue()[:-1], conn, fetchable=False)

def get_and_insert_predictions_for_batch(batch_ids_to_check, tfidf_vectorizer, 
                                         clf, prediction_table_name):
    connected = False
    while not connected:
        try:
            conn = redshift_connect()
            connected = True
        except Exception:
            sys.stderr.write('\tattempting to reconnect in 10 seconds\n')
            time.sleep()
            
    batch_ids, batch_user_posts, batch_ids_with_no_words = get_user_document_from_ids(
                                                                batch_ids_to_check, conn)
    sys.stdout.write('\tskipping {} users with no words\n'.format(
                                                           len(batch_ids_with_no_words)))
    sys.stdout.flush()
    
    # Get vector matrix using tfidf vectorizer.
    batch_vector_mx = get_post_vectors(tfidf_vectorizer, batch_user_posts, 
                                       None, cache=False)
    
    # Get predictions by applying classifier.
    decs = clf.decision_function(batch_vector_mx)
    results_df = pd.DataFrame({'id': batch_ids, 'decision': decs})

    # Send predictions back to redshift.
    insert_predictions(results_df, prediction_table_name, conn)

    # Same for null predictions.
    null_df = pd.DataFrame({'id': batch_ids_with_no_words, 
                            'decision': ['NULL']*len(batch_ids_with_no_words)})
    insert_predictions(null_df, prediction_table_name, conn)
    redshift_disconnect(conn)

def batch_num_to_ids(batch_num, rows):
    sys.stdout.write('batch {}\n'.format(batch_num+1))
    sys.stdout.flush()
    batch_start_idx = batch_num*batch_size
    batch_end_idx = (batch_num+1)*batch_size        
    batch_ids_to_check = [int(row[0]) for row in rows[batch_start_idx:batch_end_idx]]
    return batch_ids_to_check

if __name__ == '__main__':
    batch_size = 1000
    pos_label = sys.argv[1]
    neg_label = sys.argv[2]
    
    # Settings may vary for different runs of classifiers, unfortunately.
    if pos_label == 'vegan':
        model_run_suffix = '_post_aboutme_100'
        model_run_dir = '/data/model_runs/vegan{}'.format(model_run_suffix)
    elif pos_label in ['jewish', 'atheist']:
        model_run_suffix = '_{}_{}_post_1_aboutme_1'.format(pos_label, neg_label)
        model_run_dir = '/data/model_runs/{}_or_sampled'.format(model_run_suffix[1:])        
    else:
        model_run_suffix = '_{}_{}_post_1_aboutme_1'.format(pos_label, neg_label)
        model_run_dir = '/data/model_runs/{}_or'.format(model_run_suffix[1:])
    
    vocabulary_filename = model_run_dir + '/' + \
                            'tfidf_vocabulary{}.out'.format(model_run_suffix)    
    idf_filename = model_run_dir + '/' + 'idf_vector{}.out'.format(model_run_suffix)    
    tfidf_vectorizer = load_vectorizer_from_files(vocabulary_filename, idf_filename)

    sys.stdout.write('Reading in classifier\n')
    sys.stdout.flush()
    clf_filename = model_run_dir + '/' + 'linear_svc{}.out'.format(model_run_suffix)
    clf = joblib.load(clf_filename)
    
    prediction_table_name = 'user_px5_{}_updated'.format(pos_label)
    
    # Create table in redshift to store predictions.
    conn = redshift_connect()
    create_prediction_table(prediction_table_name, conn)

    sys.stdout.write('Getting all fbids\n')
    sys.stdout.flush()
    if not does_table_exist(prediction_table_name, conn):
        query = """
                SELECT fbid
                FROM users
                GROUP BY fbid
                ORDER BY fbid
                """
    else:
        query = """
                SELECT u.fbid
                FROM users u
                    LEFT JOIN {} l
                        ON u.fbid = l.fbid
                WHERE l.fbid IS NULL
                GROUP BY u.fbid
                ORDER BY u.fbid
                """.format(prediction_table_name)
    rows = execute_query(query, conn)
    redshift_disconnect(conn)
    
    total_rows = len(rows)
    for batch_num in range(total_rows / batch_size + 1):
        get_and_insert_predictions_for_batch(batch_num_to_ids(batch_num, rows), 
                                            tfidf_vectorizer, clf, prediction_table_name)
