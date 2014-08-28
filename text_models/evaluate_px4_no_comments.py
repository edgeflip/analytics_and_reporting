from db_utils import redshift_connect, redshift_disconnect, execute_query
from evaluate_user_rankings import get_post_vectors
from predict_labels_from_words import load_vectorizer_from_files
from batch_predict_label import does_table_exist
from collections import namedtuple
from cStringIO import StringIO
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import joblib
import random
import time
import sys
import re

User_Posts = namedtuple('User_Posts', ['fbid', 'posts'])

def get_edges(conn):
    query = """
            SELECT fbid_secondary, fbid_primary
            FROM fbid_edges_110000    
            """
    return execute_query(query, conn)

def create_temporary_batch_table(edges, conn):
    batch_table_name = 'batch_edges_{}'.format(random.randint(0, 100000))
    if does_table_exist(batch_table_name, conn):
        query = 'DROP TABLE {}'.format(batch_table_name)
        execute_query(query, conn, fetchable=False)
    query = 'CREATE TEMP TABLE {} (fbid_secondary bigint, fbid_primary bigint)'.format(batch_table_name) \
            + ' DISTKEY (fbid_secondary) SORTKEY (fbid_secondary, fbid_primary) '
    execute_query(query, conn, fetchable=False)
    query = StringIO()
    query.write('INSERT INTO {} VALUES '.format(batch_table_name))
    for edge in edges:
        query.write('({}, {}),'.format(edge[0], edge[1]))
    query = query.getvalue()[:-1]
    execute_query(query, conn, fetchable=False)    
    return batch_table_name

def get_px4_data(batch_edges, batch_table_name, conn):
    sys.stdout.write('\tgetting px4 data for batch...\n')
    sys.stdout.flush()
    edge_to_px4_messages = {edge: StringIO() for edge in batch_edges}
    
    query = """
            SELECT t.fbid_secondary, t.fbid_primary, t.message
            FROM (
            SELECT u.fbid_post, b.fbid_secondary, b.fbid_primary, min(p.message) as message
            FROM {batch_table_name} b
                JOIN user_posts_with_poster_110000 u ON u.fbid_user = b.fbid_secondary AND u.fbid_poster = b.fbid_primary
                JOIN posts_oldkey_110000 p ON u.fbid_post = p.fbid_post 
            GROUP BY u.fbid_post, b.fbid_secondary, b.fbid_primary) t
            """.format(batch_table_name=batch_table_name)
    rows = execute_query(query, conn)
    
    for row in rows:
        fbid_secondary, fbid_primary = row[0], row[1]
        if row[2]: # only if non-empty message
            message = ' '.join(re.split('\s+', row[2]))
            edge_to_px4_messages[(fbid_secondary, fbid_primary)].write(message)
            edge_to_px4_messages[(fbid_secondary, fbid_primary)].write(' ')
    
    edges_with_words = []
    edges_with_no_words = []
    user_posts_list = []
    for edge, message in edge_to_px4_messages.items():
        words = message.getvalue().strip()
        if not words:
            edges_with_no_words.append(edge)
        else:
            edges_with_words.append(edge)
            user_posts_list.append(User_Posts(edge, words))
    return edges_with_words, user_posts_list, edges_with_no_words

def get_px5_predictions(batch_table_name, label, conn):    
    sys.stdout.write('\tgetting px5 predictions for batch...\n')
    sys.stdout.flush()
    query = """
            SELECT u.fbid, prediction
            FROM user_px5_{label} u
                JOIN {batch_table_name} b
                  ON u.fbid = b.fbid_secondary
            """.format(label=label, batch_table_name=batch_table_name)
    return execute_query(query, conn)

if __name__ == '__main__':
    batch_size = 10000
    positive_label = sys.argv[1]
    negative_label = sys.argv[2]
    # label = 'vegan'
    model_run_suffix = '_{}_{}_post_1_aboutme_1'.format(positive_label, negative_label)
    model_run_dir = '/data/model_runs/{}_or'.format(model_run_suffix[1:])
    vocabulary_filename = model_run_dir + '/' + 'tfidf_vocabulary{}.out'.format(model_run_suffix)    
    idf_filename = model_run_dir + '/' + 'idf_vector{}.out'.format(model_run_suffix)    
    tfidf_vectorizer = load_vectorizer_from_files(vocabulary_filename, idf_filename)
    
    clf_filename = model_run_dir + '/' + 'linear_svc{}.out'.format(model_run_suffix)
    clf = joblib.load(clf_filename)
    
    # get sample of edges to evaluate
    conn = redshift_connect()
    edges = get_edges(conn)
    total_rows = len(edges)
    
    px4_predictions_df = pd.DataFrame(columns=('fbid_secondary', 'fbid_primary', 'prediction'))
    px5_predictions_df = pd.DataFrame(columns=('fbid_secondary', 'prediction'))    
    
    # get data in batches (px4 data to predict with and px5 predictions)
    for batch_num in range(total_rows / batch_size):
        sys.stdout.write('batch {}\n'.format(batch_num+1))
        batch_start_idx = batch_num*batch_size
        batch_end_idx = (batch_num+1)*batch_size        
        batch_edges = edges[batch_start_idx:batch_end_idx]
        batch_table_name = create_temporary_batch_table(batch_edges, conn)
        
        # add px5 cached results to data frame
        fbid_px5_prediction = get_px5_predictions(batch_table_name, positive_label, conn)
        px5_predictions_df = px5_predictions_df.append(pd.DataFrame({'fbid_secondary': [str(x[0]) for x in fbid_px5_prediction],
                                                                     'prediction': [x[1] for x in fbid_px5_prediction]}))
                                                                     
        batch_edges, batch_user_posts, batch_edges_with_no_words = get_px4_data(batch_edges, batch_table_name, conn)
        # add px4 NULL results to data frame (no messages for this pair of edges)
        px4_predictions_df = px4_predictions_df.append(pd.DataFrame({'fbid_secondary': [str(x[0]) for x in batch_edges_with_no_words],
                                                                     'fbid_primary': [str(x[1]) for x in batch_edges_with_no_words], 
                                                                     'prediction': [np.nan]*len(batch_edges_with_no_words)}))
        
        # predict for each batch by applying classifier
        batch_vector_mx = get_post_vectors(tfidf_vectorizer, batch_user_posts, None, cache=False)
        decs = clf.decision_function(batch_vector_mx)
        
        # add px4 results to data frame
        px4_predictions_df = px4_predictions_df.append(pd.DataFrame({'fbid_secondary': [str(x[0]) for x in batch_edges], 
                                                                     'fbid_primary': [str(x[1]) for x in batch_edges], 
                                                                     'prediction': decs}))
    
    # combine px4 and px5 results
    joined_df = pd.merge(px4_predictions_df, px5_predictions_df, on='fbid_secondary', how='inner', suffixes=('_px4', '_px5'))
    
    # evaluate performance against px5 predictions
    px5_not_nan = np.logical_not(np.isnan(joined_df['prediction_px5']))
    px4_not_nan = np.logical_not(np.isnan(joined_df['prediction_px4']))
    px4_positive = np.logical_and(px4_not_nan, joined_df['prediction_px4'] > 0)
    px5_positive = np.logical_and(px5_not_nan, joined_df['prediction_px5'] > 0)
    px4_negative = np.logical_and(px4_not_nan, joined_df['prediction_px4'] < 0)
    px5_negative = np.logical_and(px5_not_nan, joined_df['prediction_px5'] < 0)
    
    sys.stdout.write('Total users examined: {}\n'.format(len(joined_df)))
    sys.stdout.write('Proportion px4 nans: {}\n'.format(1.0 - 1.0*np.sum(px4_not_nan) / px4_not_nan.size))
    sys.stdout.write('Proportion px5 nans: {}\n'.format(1.0 - 1.0*np.sum(px5_not_nan) / px5_not_nan.size))
    sys.stdout.write('Proportion px4 nans and px5 nans: {}\n'.format(1.0*np.sum(np.logical_and(np.logical_not(px4_not_nan), np.logical_not(px5_not_nan))) / px5_not_nan.size))
    sys.stdout.write('Proportion where px4 not nan and px5 not nan: {}\n'.format(1.0*np.sum(np.logical_and(px4_not_nan, px5_not_nan)) / px5_not_nan.size))
    sys.stdout.write('Number of matches where predicted positive by both px4 and px5: {}\n'.format(np.sum(np.logical_and(px4_positive, px5_positive))))
    sys.stdout.write('Number of users where positive px4, negative px5: {}\n'.format(np.sum(np.logical_and(px4_positive, px5_negative))))
    sys.stdout.write('Number of users where positive px4, nan px5: {}\n'.format(np.sum(np.logical_and(px4_positive, np.logical_not(px5_not_nan)))))
    sys.stdout.write('Proportion that px4 says positive where px5 says positive: {}\n'.format(1.0*np.sum(np.logical_and(px4_positive, px5_positive))/np.sum(px5_positive)))
    sys.stdout.write('Number of matches where predicted negative by both px4 and px5: {}\n'.format(np.sum(np.logical_and(px4_negative, px5_negative))))
    sys.stdout.write('Proportion that px4 says negative where px5 says negative: {}\n'.format(1.0*np.sum(np.logical_and(px4_negative, px5_negative))/np.sum(px5_negative)))
    sys.stdout.write('Correlation where px4 not nan and px5 not nan: {}\n'.format(pearsonr(joined_df[px4_not_nan & px5_not_nan]['prediction_px4'], joined_df[px4_not_nan & px5_not_nan]['prediction_px5'])))
    
    sys.stdout.write('Top 10 users where px4 positive, px5 negative:\n')
    sys.stdout.write('{}\n'.format(joined_df[px4_positive & px5_negative].sort('prediction_px4', ascending=False).head(10)))
    redshift_disconnect(conn)