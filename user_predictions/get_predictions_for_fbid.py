from collections import namedtuple
from cStringIO import StringIO
import time
import sys
import re

import pandas as pd
import numpy as np
import joblib

from db_utils import redshift_connect, redshift_disconnect
from db_utils import execute_query, does_table_exist
from batch_predict_label import get_post_and_aboutme_document_from_ids_helper
from predict_utils import load_vectorizer_from_files, get_post_vectors
from predict_utils import get_post_and_aboutme_document_from_id, get_user_posts
from predict_utils import User_Posts

def get_k_most_discriminative_features(fbid_user_posts, k, label):
    dir_name, positive_label, negative_label, dir_suffix, file_suffix = label_to_files[label]
    full_dir_name = '{dir_name}/{positive_label}_{negative_label}{dir_suffix}'.format(
                            dir_name=dir_name, 
                            positive_label=positive_label, 
                            negative_label=negative_label, 
                            dir_suffix=dir_suffix)
    if label == 'vegan':
        vocabulary_filename = '{full_dir_name}/tfidf_vocabulary_{file_suffix}.out'.format(
                                full_dir_name=full_dir_name,
                                file_suffix=file_suffix)
        idf_filename = '{full_dir_name}/idf_vector_{file_suffix}.out'.format(
                                full_dir_name=full_dir_name, 
                                file_suffix=file_suffix)
        classifier_filename = '{full_dir_name}/linear_svc_{file_suffix}.out'.format(
                                full_dir_name=full_dir_name, 
                                file_suffix=file_suffix)
    else:
        vocabulary_filename = ('{full_dir_name}/tfidf_vocabulary_{positive_label}_'
                               '{negative_label}{file_suffix}.out').format(
                                full_dir_name=full_dir_name,
                                positive_label=positive_label, 
                                negative_label=negative_label, 
                                file_suffix=file_suffix)
        idf_filename = ('{full_dir_name}/idf_vector_{positive_label}_{negative_label}'
                        '{file_suffix}.out').format(
                                full_dir_name=full_dir_name, 
                                positive_label=positive_label, 
                                negative_label=negative_label, 
                                file_suffix=file_suffix)
        classifier_filename = ('{full_dir_name}/linear_svc_{positive_label}_'
                               '{negative_label}{file_suffix}.out').format(
                                full_dir_name=full_dir_name, 
                                positive_label=positive_label, 
                                negative_label=negative_label, 
                                file_suffix=file_suffix)
    
    tfidf_vectorizer = load_vectorizer_from_files(vocabulary_filename, idf_filename)
    f = np.array(tfidf_vectorizer.get_feature_names())
    
    clf = joblib.load(classifier_filename)
    coefs = clf.coef_
    
    mx = get_post_vectors(tfidf_vectorizer, fbid_user_posts, None, cache=False)
    
    values = []
    for i, fbid_user_post in enumerate(fbid_user_posts):
        fbid = fbid_user_post.fbid
        mx_row = mx[i].toarray()
        w = np.array(mx_row*coefs)
        bottom_k_vals = list(w[0][(w).argsort()[0][::1][:k]])
        bottom_k_features = list(f[(w).argsort()[0][::1][:k]])
        top_k_vals = list(w[0][(w).argsort()[0][::-1][:k]])
        top_k_features = list(f[(w).argsort()[0][::-1][:k]])
        for top_feature, top_val, bottom_feature, bottom_val in zip(
                                                            top_k_features, 
                                                            top_k_vals, 
                                                            bottom_k_features, 
                                                            bottom_k_vals):
            values.append((fbid, top_feature, top_val, bottom_feature, bottom_val))
    
    df = pd.DataFrame(data=np.array(values, dtype=('int32,object,float64,object,float64')))
    df.columns = ['fbid', 'positive_feature', 'score_p', 'negative_feature', 'score_n']
    return df

label_to_table_names = {}
label_to_table_names['female'] = 'user_px5_female_updated'
label_to_table_names['vegan'] = 'user_px5_vegan_updated'
label_to_table_names['parent'] = 'user_px5_parent_updated'
label_to_table_names['lgbt'] = 'user_px5_lgbt_updated'
label_to_table_names['veteran'] = 'user_px5_veteran_updated'
label_to_table_names['environment'] = 'user_px5_environment_updated'
label_to_table_names['african american'] = 'user_px5_african_american_100k_updated'
label_to_table_names['hispanic'] = 'user_px5_hispanic_100k_updated'
label_to_table_names['asian'] = 'user_px5_asian_100k_updated'
label_to_table_names['liberal'] = 'user_px5_liberal_updated'
label_to_table_names['christian'] = 'user_px5_christian_updated'
label_to_table_names['jewish'] = 'user_px5_jewish_updated'
label_to_table_names['muslim'] = 'user_px5_muslim_updated'

label_to_files = {} # label -> (dir_name, positive_label, negative_label, 
#                               dir_suffix, file_suffix)
label_to_files['female']            = ('/data/model_runs', 
                                       'female', 
                                       'male', 
                                       '_post_1_aboutme_1_or',
                                       '_post_1_aboutme_1')
label_to_files['vegan']             = ('/data/model_runs', 
                                       'vegan', 
                                       '', 
                                       'post_aboutme_100',
                                       'post_aboutme_100')
label_to_files['parent']            = ('/data/model_runs', 
                                       'parent', 
                                       'nonparents_100k', 
                                       '_post_1_aboutme_1_or',
                                       '_post_1_aboutme_1')
label_to_files['lgbt']             = ('/data/model_runs', 
                                      'lgbt', 
                                      '100000', 
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['veteran']          = ('/data/model_runs', 
                                      'veteran', 
                                      'nonveterans', 
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['environment']      = ('/data/model_runs', 
                                      'environment', 
                                      '100000', 
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['african american'] = ('/data/model_runs', 
                                      'african_american_100k', 
                                      '100000', 
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['hispanic']         = ('/data/model_runs',
                                      'hispanic_100k',
                                      '100000',
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['asian']            = ('/data/model_runs',
                                      'asian_100k',
                                      '100000',
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['liberal']          = ('/data/model_runs',
                                      'liberal',
                                      'conservative',
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['christian']        = ('/data/model_runs',
                                      'christian',
                                      'nonchristians',
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['jewish']           = ('/data/model_runs',
                                      'jewish',
                                      '100000',
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')
label_to_files['muslim']           = ('/data/model_runs',
                                      'muslim',
                                      '100000',
                                      '_post_1_aboutme_1_or',
                                      '_post_1_aboutme_1')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: python get_predictions_for_fbid.py ')
        sys.stderr.write('fbid [on_network] [top k features] [network_posts_cached]\n')
        sys.exit()
    
    fbid = int(sys.argv[1])
    if len(sys.argv) > 2:
        if sys.argv[2] not in ['0', '1']:
            sys.stderr.write('on_network must be in [0, 1]\n')
            sys.exit()
        on_network = int(sys.argv[2])
    else:
        on_network = 0
    
    if len(sys.argv) > 3:
        top_k = int(sys.argv[3])
    else:
        top_k = 0
    
    if len(sys.argv) > 4:
        posts_from_cache = True
        posts_cache_filename = ('/data/user_documents/individual_posts_marc_friends/'
                                'all-individual-posts.txt')
        aboutme_cache_filename = ('/data/user_documents/individual_posts_marc_friends/'
                                  'all-individual-aboutme.txt')
    else:
        posts_from_cache = False
    
    conn = redshift_connect()
    
    if not on_network:
        labels = []
        values = []
        for label, table_name in label_to_table_names.items():
            query = """
                    SELECT prediction
                    FROM {table_name}
                    WHERE fbid = {fbid}
                    """.format(table_name=table_name, fbid=fbid)
            rows = execute_query(query, conn)
            labels.append(label)
            values.append(float(rows[0][0]))
        
        prediction_df = pd.DataFrame(values, index=labels)
        prediction_df.columns = ['prediction']
        
        if top_k:
            fbid_user_post = get_post_and_aboutme_document_from_id(fbid, conn)
            fbid_user_post = User_Posts(fbid_user_post.fbid, 
                                       fbid_user_post.posts + fbid_user_post.aboutme, 
                                       '')
            dfs = []
            for label in label_to_table_names:
                sys.stdout.write('computing top features for {}...\n'.format(label))
                sys.stdout.flush()            
                df = get_k_most_discriminative_features([fbid_user_post], top_k, label)
                df['label'] = [label]*len(df)
                dfs.append(df)
            feature_df = pd.concat(dfs)
            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
            prediction_df['label'] = prediction_df.index
            prediction_with_features_df = pd.merge(prediction_df, feature_df, 
                                                   on=['label'])
            prediction_with_features_df = prediction_with_features_df.set_index(['label'])
            for label in label_to_table_names:
                print(prediction_with_features_df.ix[label].head(100))
                print('')
        else:
            print(prediction_df)
    else:
        temp_network_table_name = 'temp_network_{}'.format(fbid)
        query = """
                CREATE TEMP TABLE {temp_network_table_name} distkey (fbid) sortkey (fbid)
                AS
                    SELECT fbid, min(name) as name
                    FROM (
                        SELECT e.fbid_source AS fbid, u.fname || ' ' || u.lname AS name
                        FROM edges e
                            JOIN users u ON e.fbid_source = u.fbid
                        WHERE e.fbid_target = {fbid}
                    )
                    GROUP BY fbid
                    ORDER BY fbid
                """.format(temp_network_table_name=temp_network_table_name, 
                           fbid=fbid)
        execute_query(query, conn, fetchable=False)
        sys.stdout.write('created temp network table\n')
        sys.stdout.flush()
        
        if top_k and not posts_from_cache:
            _, fbid_user_posts, _ = get_post_and_aboutme_document_from_ids_helper(
                                                         temp_network_table_name, conn)
        elif top_k and posts_from_cache:
            fbid_user_posts = get_user_posts(posts_cache_filename, 
                                             aboutme_cache_filename)[0]
            fbid_user_posts = [User_Posts(user_posts.fbid, 
                                          user_posts.posts + ' ' + user_posts.aboutme,
                                          '')
                                    for user_posts in fbid_user_posts]
        else:
            pass
        
        labels_and_predictions = []
        for label, table_name in label_to_table_names.items():
            sys.stdout.write('getting predictions for {}...\n'.format(label))
            sys.stdout.flush()
            query = """
                    SELECT p.fbid, p.prediction, u.name
                    FROM {table_name} p
                        JOIN {temp_network_table_name} u USING (fbid)
                    WHERE prediction IS NOT NULL
                    ORDER BY prediction DESC
                    """.format(table_name=table_name, 
                               temp_network_table_name=temp_network_table_name)
            rows = execute_query(query, conn)
            for row in rows:
                labels_and_predictions.append((label, int(row[0]), 
                                               float(row[1]), str(row[2])))
        
        prediction_df = pd.DataFrame(data=np.array(labels_and_predictions, 
                                     dtype=('object,int32,float64,object')))
        prediction_df.columns = ['label', 'fbid', 'prediction', 'name']
        prediction_df = prediction_df.sort(['label', 'prediction'], 
                                           ascending=[True, False])
        
        if top_k:
            dfs = []
            for label in label_to_table_names:
                sys.stdout.write('computing top features for {}...\n'.format(label))
                sys.stdout.flush()            
                df = get_k_most_discriminative_features(fbid_user_posts, top_k, label)
                df['label'] = [label]*len(df)
                dfs.append(df)
            feature_df = pd.concat(dfs)
        
        if not top_k:
            for label in label_to_table_names:
                print(prediction_df[prediction_df['label'] == label].sort(
                                    'prediction', ascending=False).head(10))
                print('')
        else:
            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
            prediction_with_features_df = pd.merge(prediction_df, feature_df, 
                                                   on=['label', 'fbid'])
            prediction_with_features_df = prediction_with_features_df.set_index(['label'])
            for label in label_to_table_names:
                print(prediction_with_features_df.ix[label].head(100))
                print('')
        
    redshift_disconnect(conn)