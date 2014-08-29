from cStringIO import StringIO
import random
import sys
import os
import re

from sklearn.svm import LinearSVC
from scipy.sparse import vstack
import pandas as pd
import numpy as np
import joblib

from predict_utils import get_ids_from_file, get_user_posts, order_user_posts
from predict_utils import prune_ids_and_posts_by_thresholds, get_vectorizer
from predict_utils import get_post_vectors, get_names_from_fbids, User_Posts

def print_result_statistics(df, report_file, pos_user_posts, neg_user_posts):
    tok = re.compile(r'(?u)\b\w\w+\b')
    pos_words = [tok.findall(up.posts.lower()) for up in pos_user_posts]
    neg_words = [tok.findall(up.posts.lower()) for up in neg_user_posts]
    df['num_words'] = [len(words) for words in pos_words + neg_words]
    
    neg_pred_condition = df['decision'] < 0
    pos_pred_condition = df['decision'] > 0
    neg_true_condition = df['true_label'] == 0
    pos_true_condition = df['true_label'] == 1
    
    # Summary statistics about performance
    num_tn = len(df[neg_pred_condition & neg_true_condition])
    num_tp = len(df[pos_pred_condition & pos_true_condition])
    num_fn = len(df[neg_pred_condition & pos_true_condition])
    num_fp = len(df[pos_pred_condition & neg_true_condition])
    
    report_file.write('             Accuracy: {}\n'.format(1.0*(num_tp+num_tn) / len(df)))
    report_file.write('   True positive rate: {}\n'.format(1.0*num_tp / (num_tp+num_fn)))
    report_file.write('   True negative rate: {}\n'.format(1.0*num_tn / (num_tn+num_fp)))
    report_file.write('  False positive rate: {}\n'.format(1.0*num_fp / (num_fp+num_tn)))
    report_file.write('  False negative rate: {}\n'.format(1.0*num_fn / (num_fn+num_tp)))
    report_file.write(' False discovery rate: {}\n'.format(1.0*num_fp / (num_fp+num_tp)))
    report_file.write('            Precision: {}\n'.format(1.0*num_tp / (num_fp+num_tp)))
    report_file.write('               Recall: {}\n'.format(1.0*num_tp / (num_fn+num_tp)))
    report_file.write('Diagnostic odds ratio: {}\n'.format(
                                              (1.0*num_tp/num_fn) / (1.0*num_fp/num_tn)))
    report_file.write('\n')
    
    # Get count, mean score, and mean num words for a different cross-sections.
    report_file.write('Type of pred:\tN\tmean score\tmean num words\n')
    for out_label, conditions in [('Positive label', pos_true_condition),
                                  ('Negative label', neg_true_condition),
                                  ('Positive pred', pos_pred_condition),
                                  ('Negative pred', neg_pred_condition),
                                  ('True positive', pos_true_condition & 
                                                        pos_pred_condition),
                                  ('True negative', neg_true_condition & 
                                                        neg_pred_condition),
                                  ('False positive', neg_true_condition & 
                                                        pos_pred_condition),
                                  ('False negative', pos_true_condition & 
                                                        neg_pred_condition)]:
        subset = df[conditions]
        c = subset['decision'].count()
        m = subset['decision'].mean()
        w = subset['num_words'].mean()
        report_file.write('{}: {}\t{}\t{}\n'.format(out_label, c, m, w))
    report_file.write('\n')
    
    # Peek at the top positively labeled users that are true positives,
    report_file.write('Top 20 true positives\n')
    report_file.write('{}\n'.format(df[pos_true_condition & pos_pred_condition].sort(
                                'decision', ascending=False)[:20][['decision', 'id']]))
    report_file.write('\n')
    
    # true negatives,
    report_file.write('Top 20 true negatives\n')
    report_file.write('{}\n'.format(df[neg_true_condition & neg_pred_condition].sort(
                                'decision', ascending=True)[:20][['decision', 'id']]))
    report_file.write('\n')

    # false positives,
    report_file.write('Top 20 false positives\n')
    report_file.write('{}\n'.format(df[neg_true_condition & pos_pred_condition].sort(
                                'decision', ascending=False)[:20][['decision', 'id']]))
    report_file.write('\n')
    
    fbid_to_name = get_names_from_fbids(
                        list(df[neg_true_condition & pos_pred_condition]['id']))
    for row in df[neg_true_condition & pos_pred_condition].sort(
                                                'decision', ascending=False).iterrows():
        if str(row[1]['id']) in fbid_to_name:
            report_file.write('{}\t{}\t{}\n'.format(row[1]['id'], row[1]['decision'], 
                                                    fbid_to_name[str(row[1]['id'])]))
    report_file.write('\n')
    
    # and false negatives.
    report_file.write('Top 20 false negatives\n')
    report_file.write('{}\n'.format(df[pos_true_condition & neg_pred_condition].sort(
                                    'decision', ascending=True)[:20][['decision', 'id']]))

if __name__ == '__main__':
    if len(sys.argv) < 5:
        sys.stderr.write('python predict_labels_from_words positive_label ')
        sys.stderr.write('negative_label word_threshold aboutme_threshold\n')
        sys.stderr.write('\t pos_label: the label of positive instances (e.g., female)\n')
        sys.stderr.write('\t neg_label: the label of negative instances (e.g., male)\n')
        sys.stderr.write('\t post_threshold: None or integer for minimum number ')
        sys.stderr.write('of characters in posts for training data\n')
        sys.stderr.write('\t aboutme_threshold: None or integer for minimum number ')
        sys.stderr.write('of characters in aboutme for training data\n')
        sys.exit()
    
    pos_label = sys.argv[1]
    neg_label = sys.argv[2]
    post_threshold = None if sys.argv[3] == 'None' else int(sys.argv[3])
    aboutme_threshold = None if sys.argv[4] == 'None' else int(sys.argv[4])
    use_sampled_train = False if len(sys.argv) < 5 else sys.argv[5] == 'True'
    
    neg_dir = '/data/user_documents/individual_posts_{}'.format(neg_label)
    pos_dir = '/data/user_documents/individual_posts_{}'.format(pos_label)
    model_run_suffix = '_{}_{}_post{}_aboutme{}'.format(
                            pos_label, neg_label, 
                            '_{}'.format(post_threshold) if post_threshold else '',
                            '_{}'.format(aboutme_threshold) if aboutme_threshold else '')
    if use_sampled_train:
        model_run_dir = '/data/model_runs/{}_or_sampled'.format(model_run_suffix[1:])
    else:
        model_run_dir = '/data/model_runs/{}_or'.format(model_run_suffix[1:])
    if not os.path.exists(model_run_dir):
        os.makedirs(model_run_dir)
    
    report_filename = 'report{}.out'.format(model_run_suffix)
    report_file = open(model_run_dir + '/' + report_filename, 'w')
    
    # Get positive train/test user ids.
    report_file.write('Getting positive training and test ids\n')
    report_file.flush()
    pos_train_ids_filename = pos_dir + '/' + 'train-user-ids{}.txt'.format(
                                            '-sample' if use_sampled_train else '')
    pos_train_ids = get_ids_from_file(pos_train_ids_filename)
    pos_test_ids_filename = pos_dir + '/' + 'test-user-ids.txt'
    pos_test_ids = get_ids_from_file(pos_test_ids_filename)
    
    # Get negative train/test user ids.
    report_file.write('Getting negative training and test ids\n')
    report_file.flush()
    neg_train_ids_filename = neg_dir + '/' + \
                                'train-user-ids{}.txt'.format(
                                  '-sample-for-{}'.format(pos_label) if use_sampled_train
                                  else '')
    neg_train_ids = get_ids_from_file(neg_train_ids_filename)
    neg_test_ids_filename = neg_dir + '/' + 'test-user-ids.txt'
    neg_test_ids = get_ids_from_file(neg_test_ids_filename)
    
    # Remove any positive id that shows up in the negative ids.
    pos_ids = set(pos_train_ids) | set(pos_test_ids)
    train_overlap = len(set(neg_train_ids) & pos_ids)
    test_overlap = len(set(neg_test_ids) & pos_ids)
    neg_train_ids = list(set(neg_train_ids) - pos_ids)
    neg_test_ids = list(set(neg_test_ids) - pos_ids)
    report_file.write(('\tRemoved {} and {} negative ids that appeared in '
                        'positive list\n').format(train_overlap, test_overlap))
    report_file.flush()
    
    # Get positive train/test user posts.
    report_file.write('Getting positive training and test posts\n')
    report_file.flush()
    pos_post_filename = pos_dir + '/' + 'all-individual-posts.txt'
    pos_aboutme_filename = pos_dir + '/' + 'all-individual-aboutme.txt'
    pos_train_user_posts, pos_test_user_posts = get_user_posts(pos_post_filename, 
                                                               pos_aboutme_filename,
                                                               set(pos_train_ids),
                                                               set(pos_test_ids))
    
    # Get negative train/test user posts.
    report_file.write('Getting negative training and test posts\n')
    report_file.flush()
    neg_post_filename = neg_dir + '/' + 'all-individual-posts.txt'
    neg_aboutme_filename = neg_dir + '/' + 'all-individual-aboutme.txt'
    neg_train_user_posts, neg_test_user_posts = get_user_posts(neg_post_filename,
                                                               neg_aboutme_filename,
                                                               set(neg_train_ids),
                                                               set(neg_test_ids))

    # Get ids of additional positive train taken from negative train,
    # positive test from negative test, negative train from positive train,
    # and negative test from positive test that have been manually identified.
    pos_train_from_neg_ids_filename = pos_dir + '/' + \
                                        'positive-train-from-negatives-user-ids.txt'
    neg_train_from_pos_ids_filename = pos_dir + '/' + \
                                        'negative-train-from-positives-user-ids.txt'
    pos_test_from_neg_ids_filename = pos_dir + '/' + \
                                        'positive-test-from-negatives-user-ids.txt'
    neg_test_from_pos_ids_filename = pos_dir + '/' + \
                                        'negative-test-from-positives-user-ids.txt'
    pos_train_from_neg_ids = get_ids_from_file(pos_train_from_neg_ids_filename)
    neg_train_from_pos_ids = get_ids_from_file(neg_train_from_pos_ids_filename)
    pos_test_from_neg_ids = get_ids_from_file(pos_test_from_neg_ids_filename)
    neg_test_from_pos_ids = get_ids_from_file(neg_test_from_pos_ids_filename)
    report_file.write('\trerouting {} negative train ids to positive\n'.format(
                                                            len(pos_train_from_neg_ids)))
    report_file.write('\trerouting {} positive train ids to negative\n'.format(
                                                            len(neg_train_from_pos_ids)))
    report_file.write('\trerouting {} negative test ids to positive\n'.format(
                                                            len(pos_test_from_neg_ids)))
    report_file.write('\trerouting {} positive test ids to negative\n'.format(
                                                            len(neg_test_from_pos_ids)))
    report_file.flush()
    
    # Order user posts by order of ids, rerouting instances 
    # based on the manually identified mix-ups above.
    pos_train_ids = list(set(pos_train_ids + pos_train_from_neg_ids) - 
                        set(neg_train_from_pos_ids))
    pos_test_ids = list(set(pos_test_ids + pos_test_from_neg_ids) - 
                        set(neg_test_from_pos_ids))
    neg_train_ids = list(set(neg_train_ids + neg_train_from_pos_ids) - 
                        set(pos_train_from_neg_ids))
    neg_test_ids =  list(set(neg_test_ids + neg_test_from_pos_ids) - 
                        set(pos_test_from_neg_ids))
    
    pos_train_temp = order_user_posts(pos_train_user_posts + neg_train_user_posts, 
                                      pos_train_ids)
    pos_test_temp = order_user_posts(pos_test_user_posts + neg_test_user_posts, 
                                     pos_test_ids)
    neg_train_temp = order_user_posts(neg_train_user_posts + pos_train_user_posts, 
                                      neg_train_ids)
    neg_test_temp = order_user_posts(neg_test_user_posts + pos_test_user_posts, 
                                     neg_test_ids)
    
    pos_train_user_posts, pos_test_user_posts = pos_train_temp, pos_test_temp
    neg_train_user_posts, neg_test_user_posts = neg_train_temp, neg_test_temp
    
    # Remove ids and posts where number of words < threshold.
    if post_threshold or aboutme_threshold:
        report_file.write('Pruning ids/posts based on word thresholds\n')
        report_file.flush()
        neg_train_ids, neg_train_user_posts = prune_ids_and_posts_by_thresholds(
                                                    neg_train_ids, neg_train_user_posts,
                                                    post_threshold, aboutme_threshold,
                                                    'negative train',
                                                    report_file=report_file)
        pos_train_ids, pos_train_user_posts = prune_ids_and_posts_by_thresholds(
                                                    pos_train_ids, pos_train_user_posts,
                                                    post_threshold, aboutme_threshold,
                                                    'positive train',
                                                    report_file=report_file)
        neg_test_ids, neg_test_user_posts = prune_ids_and_posts_by_thresholds(
                                                    neg_test_ids, neg_test_user_posts, 
                                                    post_threshold, aboutme_threshold,
                                                    'negative test',
                                                    report_file=report_file)
        pos_test_ids, pos_test_user_posts = prune_ids_and_posts_by_thresholds(
                                                    pos_test_ids, pos_test_user_posts, 
                                                    post_threshold, aboutme_threshold,
                                                    'positive test',
                                                    report_file=report_file)
    
    # Combine post and aboutme before creating feature vectors
    pos_train_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + 
                                                            user_posts.aboutme, '') 
                                for user_posts in pos_train_user_posts]
    neg_train_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + 
                                                            user_posts.aboutme, '') 
                                for user_posts in neg_train_user_posts]
    pos_test_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + 
                                                            user_posts.aboutme, '') 
                                for user_posts in pos_test_user_posts]
    neg_test_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + 
                                                            user_posts.aboutme, '') 
                                for user_posts in neg_test_user_posts]
    
    # Train a tfidf vectorizer.
    report_file.write(('Getting Tfidf Vectorizer trained on a sample of '
                        'positive and negative posts\n'))
    report_file.flush()
    vocab_filename = model_run_dir + '/' + \
                        'tfidf_vocabulary{}.out'.format(model_run_suffix)
    idf_filename = model_run_dir + '/' + \
                        'idf_vector{}.out'.format(model_run_suffix)
    bad_phrases_filename = pos_dir + '/' + 'bad_phrases.txt'
    if not os.path.isfile(bad_phrases_filename):
        bad_phrases_filename = None
    neg_train_vector_matrix_filename = model_run_dir + '/' + \
                        'negative_train_vector_matrix{}.mtx'.format(model_run_suffix)
    tfidf_vectorizer = get_vectorizer(vocab_filename, idf_filename, 
                                      neg_train_user_posts + pos_train_user_posts, 
                                      neg_train_vector_matrix_filename,
                                      report_file=report_file,
                                      bad_phrases_filename=bad_phrases_filename)
    
    # Build word vectors for all of sets.
    report_file.write('Getting vectors for positive train posts\n')
    report_file.flush()
    pos_train_vector_matrix_filename = model_run_dir + '/' + \
                        'positive_train_vector_matrix{}.mtx'.format(model_run_suffix)
    pos_train_vector_mx = get_post_vectors(tfidf_vectorizer, pos_train_user_posts,
                                           pos_train_vector_matrix_filename,
                                           report_file=report_file)
    report_file.write('Getting vectors for positive test posts\n')
    report_file.flush()
    pos_test_vector_matrix_filename = model_run_dir + '/' + \
                        'positive_test_vector_matrix{}.mtx'.format(model_run_suffix)
    pos_test_vector_mx = get_post_vectors(tfidf_vectorizer, pos_test_user_posts, 
                                          pos_test_vector_matrix_filename,
                                          report_file=report_file)    
    report_file.write('Getting vectors for negative train posts\n')
    report_file.flush()
    neg_train_vector_mx = get_post_vectors(tfidf_vectorizer, neg_train_user_posts,
                                           neg_train_vector_matrix_filename,
                                           report_file=report_file)    
    report_file.write('Getting vectors for negative test posts\n')
    report_file.flush()
    neg_test_vector_matrix_filename = model_run_dir + '/' + \
                        'negative_test_vector_matrix{}.mtx'.format(model_run_suffix)
    neg_test_vector_mx = get_post_vectors(tfidf_vectorizer, neg_test_user_posts, 
                                          neg_test_vector_matrix_filename,
                                          report_file=report_file)
    
    # Stack positive/negative matrices for train/test sets.
    report_file.write('Stacking data\n')    
    X_train = vstack([pos_train_vector_mx, neg_train_vector_mx])
    X_test = vstack([pos_test_vector_mx, neg_test_vector_mx])
    y_train = np.concatenate((np.repeat([1], pos_train_vector_mx.shape[0]), 
                              np.repeat([0], neg_train_vector_mx.shape[0])))
    y_test = np.concatenate((np.repeat([1], pos_test_vector_mx.shape[0]), 
                              np.repeat([0], neg_test_vector_mx.shape[0])))
    
    report_file.write('Training classifier\n')
    clf_filename = model_run_dir + '/' + 'linear_svc{}.out'.format(model_run_suffix)
    if os.path.isfile(clf_filename):
        report_file.write('\tReading in cached classifier\n')
        clf = joblib.load(clf_filename)
    else:
        clf = LinearSVC(C=1)
        clf.fit(X_train, y_train)
        joblib.dump(clf, clf_filename)

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    
    report_file.write('{} total features\n'.format(len(clf.coef_[0])))    
    report_file.write('Top 100 positive discriminative features:\n')
    report_file.flush()
    coef_df = pd.DataFrame({'coefficients': clf.coef_[0], 
                            'words': tfidf_vectorizer.get_feature_names()})
    report_file.write('{}\n'.format(
                        coef_df.sort('coefficients', ascending=False).head(100)))
    report_file.write('\nTop 100 negative discriminative features:\n')
    report_file.write('{}\n'.format(coef_df.sort('coefficients').head(100)))
    report_file.write('\n')
        
    # Training stats
    decs = clf.decision_function(X_train)
    train_ids = pos_train_ids + neg_train_ids
    report_file.write('{}\t{}\t{}\n'.format(len(train_ids), len(decs), len(y_train)))
    report_file.flush()
    results_df = pd.DataFrame({'id': train_ids, 'decision': decs, 'true_label': y_train})
    print_result_statistics(results_df, report_file, 
                            pos_train_user_posts, neg_train_user_posts)

    # Testing stats
    decs = clf.decision_function(X_test)
    test_ids = pos_test_ids + neg_test_ids
    report_file.write('{}\t{}\t{}\n'.format(len(test_ids), len(decs), len(y_test)))
    report_file.flush()
    results_df = pd.DataFrame({'id': test_ids, 'decision': decs, 'true_label': y_test})    
    print_result_statistics(results_df, report_file, 
                            pos_test_user_posts, neg_test_user_posts)

    report_file.close()