from db_utils import redshift_connect, redshift_disconnect, execute_query, does_table_exist
from evaluate_user_rankings import *
from sklearn.svm import LinearSVC
from scipy.io import mmwrite
from cStringIO import StringIO
import pandas as pd
import random
import os

def load_vectorizer_from_files(vocabulary_filename, idf_filename):
    vocab = joblib.load(vocabulary_filename)
    idf = joblib.load(idf_filename)
    tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_df=0.25, min_df=0.01, vocabulary=vocab)
    n_features = idf.shape[0]
    tfidf_vectorizer._tfidf._idf_diag = spdiags(idf, diags=0, m=n_features, n=n_features)
    return tfidf_vectorizer    

def get_vectorizer(vocabulary_filename, idf_filename, user_posts_list, negative_train_vector_matrix_filename, report_file=sys.stdout, bad_phrases_filename=None):
    if os.path.isfile(vocabulary_filename) and os.path.isfile(idf_filename):
        report_file.write('\tReading in cached tfidf vectorizer\n')
        report_file.flush()
        return load_vectorizer_from_files(vocabulary_filename, idf_filename)
    elif os.path.isfile(vocabulary_filename) and not os.path.isfile(idf_filename):
        vocab = joblib.load(vocabulary_filename)
        if os.path.isfile(bad_phrases_filename):
            bad_phrases_file = open(bad_phrases_filename, 'r')
            bad_phrases = {line.strip() for line in bad_phrases_file}
            vocab = [v for v in vocab.keys() if v not in bad_phrases]
        tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_df=0.25, min_df=0.01, vocabulary=vocab)
        training_posts = [user_posts.posts for user_posts in user_posts_list]
        random.shuffle(training_posts)
        tfidf_vectorizer.fit(training_posts[:20000])
        # mx = tfidf_vectorizer.fit_transform(random.sample(training_posts, 20000))
        # mmwrite(negative_train_vector_matrix_filename, mx)
        joblib.dump(tfidf_vectorizer.vocabulary_, vocabulary_filename)
        joblib.dump(tfidf_vectorizer.idf_, idf_filename)
        tfidf_vectorizer = load_vectorizer_from_files(vocabulary_filename, idf_filename)
        return tfidf_vectorizer
    else:
        # No vectorizer cached, fit a new one to the given user-posts and cache their canonical vector
        tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_df=0.25, min_df=0.01, stop_words=['id', 'name'])
        training_posts = [user_posts.posts for user_posts in user_posts_list]
        random.shuffle(training_posts)
        tfidf_vectorizer.fit(training_posts[:20000])
        # mx = tfidf_vectorizer.fit_transform(random.sample(training_posts, 20000))
        # mmwrite(negative_train_vector_matrix_filename, mx)
        joblib.dump(tfidf_vectorizer.vocabulary_, vocabulary_filename)
        joblib.dump(tfidf_vectorizer.idf_, idf_filename)
        tfidf_vectorizer = load_vectorizer_from_files(vocabulary_filename, idf_filename)
        return tfidf_vectorizer

def prune_ids_and_posts_by_word_threshold(ids, user_posts, word_threshold, aboutme_threshold, link_desc_threshold, label):
    temp_ids_pruned = []
    temp_user_posts_pruned = []
    word_threshold = 0 if not word_threshold else word_threshold
    aboutme_threshold = 0 if not aboutme_threshold else aboutme_threshold
    link_desc_threshold = 0 if not link_desc_threshold else link_desc_threshold
    for user_id, user_post in zip(ids, user_posts):
        if len(user_post.posts) >= word_threshold or len(user_post.aboutme) >= aboutme_threshold or len(user_post.link_desc) >= link_desc_threshold:
            temp_ids_pruned.append(user_id)
            temp_user_posts_pruned.append(user_post)
    report_file.write('\tremoved {} {}\n'.format(len(ids) - len(temp_ids_pruned), label))
    ids = temp_ids_pruned[:]
    user_posts = temp_user_posts_pruned[:]
    return ids, user_posts

def print_result_statistics(results_df, report_file, positive_user_posts, negative_user_posts):
    tok = re.compile(r'(?u)\b\w\w+\b')
    pos_words = [tok.findall(up.posts.lower()) for up in positive_user_posts]
    neg_words = [tok.findall(up.posts.lower()) for up in negative_user_posts]
    results_df['num_words'] = [len(words) for words in pos_words+neg_words]
    
    neg_pred_condition = results_df['decision'] < 0
    pos_pred_condition = results_df['decision'] > 0
    neg_true_condition = results_df['true_label'] == 0
    pos_true_condition = results_df['true_label'] == 1
    
    # summary statistics about performance
    num_tn = len(results_df[neg_pred_condition & neg_true_condition])
    num_tp = len(results_df[pos_pred_condition & pos_true_condition])
    num_fn = len(results_df[neg_pred_condition & pos_true_condition])
    num_fp = len(results_df[pos_pred_condition & neg_true_condition])
    
    report_file.write('             Accuracy: {}\n'.format(1.0*(num_tp+num_tn) / len(results_df)))
    report_file.write('   True positive rate: {}\n'.format(1.0*num_tp / (num_tp+num_fn)))
    report_file.write('   True negative rate: {}\n'.format(1.0*num_tn / (num_tn+num_fp)))
    report_file.write('  False positive rate: {}\n'.format(1.0*num_fp / (num_fp+num_tn)))
    report_file.write('  False negative rate: {}\n'.format(1.0*num_fn / (num_fn+num_tp)))
    report_file.write(' False discovery rate: {}\n'.format(1.0*num_fp / (num_fp+num_tp)))
    report_file.write('            Precision: {}\n'.format(1.0*num_tp / (num_fp+num_tp)))
    report_file.write('               Recall: {}\n'.format(1.0*num_tp / (num_fn+num_tp)))
    report_file.write('Diagnostic odds ratio: {}\n'.format((1.0*num_tp/num_fn) / (1.0*num_fp/num_tn)))
    report_file.write('\n')
    
    # get count, mean score, mean num words for a different cross-sections
    report_file.write('Type of pred:\tN\tmean score\tmean num words\n')
    for out_label, conditions in [('Positive label', pos_true_condition),
                                  ('Negative label', neg_true_condition),
                                  ('Positive pred', pos_pred_condition),
                                  ('Negative pred', neg_pred_condition),
                                  ('True positive', pos_true_condition & pos_pred_condition),
                                  ('True negative', neg_true_condition & neg_pred_condition),
                                  ('False positive', neg_true_condition & pos_pred_condition),
                                  ('False negative', pos_true_condition & neg_pred_condition)]:
        subset = results_df[conditions]
        c = subset['decision'].count()
        m = subset['decision'].mean()
        w = subset['num_words'].mean()
        report_file.write('{}: {}\t{}\t{}\n'.format(out_label, c, m, w))
    report_file.write('\n')
    
    # peek at the top positively labeled users that are true positives:
    report_file.write('Top 20 true positives\n')
    report_file.write('{}\n'.format(results_df[pos_true_condition & pos_pred_condition].sort(
                                    'decision', ascending=False)[:20][['decision', 'id']]))
    report_file.write('\n')
    
    # and true negatives:
    report_file.write('Top 20 true negatives\n')
    report_file.write('{}\n'.format(results_df[neg_true_condition & neg_pred_condition].sort(
                                    'decision', ascending=True)[:20][['decision', 'id']]))
    report_file.write('\n')

    # and false positives:
    report_file.write('Top 20 false positives\n')
    report_file.write('{}\n'.format(results_df[neg_true_condition & pos_pred_condition].sort(
                                    'decision', ascending=False)[:20][['decision', 'id']]))
    report_file.write('\n')
    
    fbid_to_name = get_names_from_fbids(list(results_df[neg_true_condition & pos_pred_condition]['id']))
    for row in results_df[neg_true_condition & pos_pred_condition].sort('decision', ascending=False).iterrows():
        if str(row[1]['id']) in fbid_to_name:
            report_file.write('{}\t{}\t{}\n'.format(row[1]['id'], row[1]['decision'], fbid_to_name[str(row[1]['id'])]))
    report_file.write('\n')
    
    # and false negatives:
    report_file.write('Top 20 false negatives\n')
    report_file.write('{}\n'.format(results_df[pos_true_condition & neg_pred_condition].sort(
                                    'decision', ascending=True)[:20][['decision', 'id']]))

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

if __name__ == '__main__':
    if len(sys.argv) < 7:
        sys.stderr.write('python predict_labels_from_words positive_label negative_label prune_test word_threshold aboutme_threshold link_desc_threshold\n')
        sys.stderr.write('\t positive label: the label of positive instances (e.g., female)\n')
        sys.stderr.write('\t negative label: the label of negative instances (e.g., male)\n')
        sys.stderr.write('\t prune_test: boolean for whether to prune test data the same as the training data\n')
        sys.stderr.write('\t word_threshold: None or integer for minimum number of characters in posts for training data\n')
        sys.stderr.write('\t aboutme_threshold: None or integer for minimum number of characters in aboutme for training data\n')
        sys.stderr.write('\t link_desc_threshold: None or integer for minimum number of characters in link_description for training data\n')
        sys.exit()
    
    positive_label = sys.argv[1]
    negative_label = sys.argv[2]
    prune_test = sys.argv[3] == 'True'
    word_threshold = None if sys.argv[4] == 'None' else int(sys.argv[4])
    aboutme_threshold = None if sys.argv[5] == 'None' else int(sys.argv[5])
    link_desc_threshold = None if sys.argv[6] == 'None' else int(sys.argv[6])
    
    negative_dir = '/data/user_documents/individual_posts_{}'.format(negative_label)
    positive_dir = '/data/user_documents/individual_posts_{}'.format(positive_label)
    model_run_suffix = '_{}_{}_post{}_aboutme{}'.format(positive_label, negative_label, 
                                                        '_{}'.format(word_threshold) if word_threshold else '',
                                                        '_{}'.format(aboutme_threshold) if aboutme_threshold else '')
#     model_run_suffix = '_{}_{}_post{}_aboutme{}_link_desc{}'.format(positive_label, negative_label, 
#                                                         '_{}'.format(word_threshold) if word_threshold else '',
#                                                         '_{}'.format(aboutme_threshold) if aboutme_threshold else '',
#                                                         '_{}'.format(link_desc_threshold) if link_desc_threshold else '')
    model_run_dir = '/data/model_runs/{}_or'.format(model_run_suffix[1:])
    if not os.path.exists(model_run_dir):
        os.makedirs(model_run_dir)
    
    report_filename = 'report{}{}.out'.format(model_run_suffix, '_prune_test' if prune_test else '')
    report_file = open(model_run_dir + '/' + report_filename, 'w')
    
    # Get positive train/test user ids
    report_file.write('Getting positive training and test ids\n')
    report_file.flush()
    positive_train_ids_filename = positive_dir + '/' + 'train-user-ids.txt'
    positive_train_ids = get_ids_from_file(positive_train_ids_filename)
    positive_test_ids_filename = positive_dir + '/' + 'test-user-ids.txt'
    positive_test_ids = get_ids_from_file(positive_test_ids_filename)
    
    # Add in additional positive train taken from negative train set found to actually be positive
    # make sure to remove them from negative train. and vice versa
    positive_train_from_negatives_ids_filename = positive_dir + '/' + 'positive-train-from-negatives-user-ids.txt'
    if os.path.isfile(positive_train_from_negatives_ids_filename):
        positive_train_from_negatives_ids = get_ids_from_file(positive_train_from_negatives_ids_filename)
        report_file.write('\trerouting {} negative train ids to positive\n'.format(len(positive_train_from_negatives_ids)))
        report_file.flush()        
    else:
        positive_train_from_negatives_ids = []
            
    negative_train_from_positives_ids_filename = positive_dir + '/' + 'negative-train-from-positives-user-ids.txt'
    if os.path.isfile(negative_train_from_positives_ids_filename):
        negative_train_from_positives_ids = get_ids_from_file(negative_train_from_positives_ids_filename)
        report_file.write('\trerouting {} positive train ids to negative\n'.format(len(negative_train_from_positives_ids)))
        report_file.flush()        
    else:
        negative_train_from_positives_ids = []

    positive_test_from_negatives_ids_filename = positive_dir + '/' + 'positive-test-from-negatives-user-ids.txt'
    if os.path.isfile(positive_test_from_negatives_ids_filename):
        positive_test_from_negatives_ids = get_ids_from_file(positive_test_from_negatives_ids_filename)
        report_file.write('\trerouting {} negative test ids to positive\n'.format(len(positive_test_from_negatives_ids)))
        report_file.flush()        
    else:
        positive_test_from_negatives_ids = []

    negative_test_from_positives_ids_filename = positive_dir + '/' + 'negative-test-from-positives-user-ids.txt'
    if os.path.isfile(negative_test_from_positives_ids_filename):
        negative_test_from_positives_ids = get_ids_from_file(negative_test_from_positives_ids_filename)
        report_file.write('\trerouting {} positive test ids to negative\n'.format(len(negative_test_from_positives_ids)))
        report_file.flush()        
    else:
        negative_test_from_positives_ids = []
        
    # Get negative train/test user ids
    report_file.write('Getting negative training and test ids\n')
    report_file.flush()
    negative_train_ids_filename = negative_dir + '/' + 'train-user-ids.txt'
    negative_train_ids = get_ids_from_file(negative_train_ids_filename)
    negative_test_ids_filename = negative_dir + '/' + 'test-user-ids.txt'
    negative_test_ids = get_ids_from_file(negative_test_ids_filename)
    
    # Remove any positive id that shows up in the negative ids
    positive_ids = set(positive_train_ids) | set(positive_test_ids)
    train_overlap = len(set(negative_train_ids) & positive_ids)
    test_overlap = len(set(negative_test_ids) & positive_ids)
    report_file.write('\tRemoved {} and {} negative ids that appeared in positive list\n'.format(train_overlap, test_overlap))
    report_file.flush()
    negative_train_ids = list(set(negative_train_ids) - positive_ids)
    negative_test_ids = list(set(negative_test_ids) - positive_ids)
    
    # Get positive train/test user posts
    report_file.write('Getting positive training and test posts\n')
    report_file.flush()
    positive_post_filename = positive_dir + '/' + 'all-individual-posts.txt'
    positive_aboutme_filename = positive_dir + '/' + 'all-individual-aboutme.txt'
    positive_link_desc_filename = positive_dir + '/' + 'all-individual-links-and-descriptions.txt'
    positive_train_user_posts, positive_test_user_posts = get_user_posts(positive_post_filename, 
                                                                         positive_aboutme_filename,
                                                                         None, #positive_link_desc_filename,
                                                                         set(positive_train_ids),
                                                                         set(positive_test_ids))
    
    
    # Get negative train/test user posts
    report_file.write('Getting negative training and test posts\n')
    report_file.flush()
    negative_post_filename = negative_dir + '/' + 'all-individual-posts.txt'
    negative_aboutme_filename = negative_dir + '/' + 'all-individual-aboutme.txt'
    negative_link_desc_filename = negative_dir + '/' + 'all-individual-links-and-descriptions.txt'
    negative_train_user_posts, negative_test_user_posts = get_user_posts(negative_post_filename,
                                                                         negative_aboutme_filename,
                                                                         None,#negative_link_desc_filename,
                                                                         set(negative_train_ids),
                                                                         set(negative_test_ids))

    # order user posts by order of ids, rerouting some instances based on manually identified mix-ups
    positive_train_ids = list(set(positive_train_ids + positive_train_from_negatives_ids) - set(negative_train_from_positives_ids))
    positive_test_ids = list(set(positive_test_ids + positive_test_from_negatives_ids) - set(negative_test_from_positives_ids))
    negative_train_ids = list(set(negative_train_ids + negative_train_from_positives_ids) - set(positive_train_from_negatives_ids))
    negative_test_ids =  list(set(negative_test_ids + negative_test_from_positives_ids) - set(positive_test_from_negatives_ids))
    
    positive_train_user_posts_new = order_user_posts(positive_train_user_posts+negative_train_user_posts, positive_train_ids)
    positive_test_user_posts_new = order_user_posts(positive_test_user_posts+negative_test_user_posts, positive_test_ids)
    negative_train_user_posts_new = order_user_posts(negative_train_user_posts+positive_train_user_posts, negative_train_ids)
    negative_test_user_posts_new = order_user_posts(negative_test_user_posts+positive_test_user_posts, negative_test_ids)
    
    positive_train_user_posts = positive_train_user_posts_new
    positive_test_user_posts = positive_test_user_posts_new
    negative_train_user_posts = negative_train_user_posts_new
    negative_test_user_posts = negative_test_user_posts_new
    
    # Remove ids and posts where number of words < threshold
    if word_threshold or aboutme_threshold:
        report_file.write('Pruning ids/posts based on word thresholds\n')
        report_file.flush()
        negative_train_ids, negative_train_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            negative_train_ids, 
                                                            negative_train_user_posts,
                                                            word_threshold, 
                                                            aboutme_threshold,
                                                            link_desc_threshold,
                                                            'negative train')
        positive_train_ids, positive_train_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            positive_train_ids, 
                                                            positive_train_user_posts,
                                                            word_threshold, 
                                                            aboutme_threshold,
                                                            link_desc_threshold,
                                                            'positive train')
        if prune_test:
            negative_test_ids, negative_test_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            negative_test_ids, 
                                                            negative_test_user_posts, 
                                                            word_threshold,
                                                            aboutme_threshold,
                                                            link_desc_threshold,
                                                            'negative test')
            positive_test_ids, positive_test_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            positive_test_ids, 
                                                            positive_test_user_posts, 
                                                            word_threshold,
                                                            aboutme_threshold,
                                                            link_desc_threshold,
                                                            'positive test')
    
    # Combine post and aboutme before creating feature vectors
    positive_train_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + user_posts.aboutme + ' ' + user_posts.link_desc, '', '') for user_posts in positive_train_user_posts]
    negative_train_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + user_posts.aboutme + ' ' + user_posts.link_desc, '', '') for user_posts in negative_train_user_posts]
    positive_test_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + user_posts.aboutme + ' ' + user_posts.link_desc, '', '') for user_posts in positive_test_user_posts]
    negative_test_user_posts = [User_Posts(user_posts.fbid, user_posts.posts + ' ' + user_posts.aboutme + ' ' + user_posts.link_desc, '', '') for user_posts in negative_test_user_posts]
    
    # Load in tfidf vectorizer    
    report_file.write('Getting Tfidf Vectorizer trained on a sample of positive and negative posts\n')
    report_file.flush()
    vocabulary_filename = model_run_dir + '/' + 'tfidf_vocabulary{}.out'.format(model_run_suffix)
    idf_filename = model_run_dir + '/' + 'idf_vector{}.out'.format(model_run_suffix)
    bad_phrases_filename = positive_dir + '/' + 'bad_phrases.txt'
    if not os.path.isfile(bad_phrases_filename):
        bad_phrases_filename = None
    negative_train_vector_matrix_filename = model_run_dir + '/' + 'negative_train_vector_matrix{}.mtx'.format(model_run_suffix)
    tfidf_vectorizer = get_vectorizer(vocabulary_filename, idf_filename, 
                                      negative_train_user_posts + positive_train_user_posts, 
                                      negative_train_vector_matrix_filename,
                                      report_file=report_file,
                                      bad_phrases_filename=bad_phrases_filename)
    
    # Build word vectors for all of them
    report_file.write('Getting vectors for positive train posts\n')
    report_file.flush()
    positive_train_vector_matrix_filename = model_run_dir + '/' + 'positive_train_vector_matrix{}.mtx'.format(model_run_suffix)
    positive_train_vector_mx = get_post_vectors(tfidf_vectorizer, positive_train_user_posts,
                                               positive_train_vector_matrix_filename)
    report_file.write('Getting vectors for positive test posts\n')
    report_file.flush()
    positive_test_vector_matrix_filename = model_run_dir + '/' + 'positive_test_vector_matrix{}{}.mtx'.format(model_run_suffix, '_prune_test' if prune_test else '')
    positive_test_vector_mx = get_post_vectors(tfidf_vectorizer, positive_test_user_posts, 
                                               positive_test_vector_matrix_filename)
    
    report_file.write('Getting vectors for negative train posts\n')
    report_file.flush()
    negative_train_vector_mx = get_post_vectors(tfidf_vectorizer, negative_train_user_posts,
                                               negative_train_vector_matrix_filename)
    
    report_file.write('Getting vectors for negative test posts\n')
    report_file.flush()
    negative_test_vector_matrix_filename = model_run_dir + '/' + 'negative_test_vector_matrix{}{}.mtx'.format(model_run_suffix, '_prune_test' if prune_test else '')
    negative_test_vector_mx = get_post_vectors(tfidf_vectorizer, negative_test_user_posts, 
                                               negative_test_vector_matrix_filename)
    
    # Stack positive/negative for train/test
    report_file.write('Stacking data\n')    
    X_train = vstack([positive_train_vector_mx, negative_train_vector_mx])
    X_test = vstack([positive_test_vector_mx, negative_test_vector_mx])
    y_train = np.concatenate((np.repeat([1], positive_train_vector_mx.shape[0]), 
                              np.repeat([0], negative_train_vector_mx.shape[0])))
    y_test = np.concatenate((np.repeat([1], positive_test_vector_mx.shape[0]), 
                              np.repeat([0], negative_test_vector_mx.shape[0])))
    
    report_file.write('Training classifier\n')
    clf_filename = model_run_dir + '/' + 'linear_svc{}.out'.format(model_run_suffix)
    if os.path.isfile(clf_filename):
        report_file.write('\tReading in cached classifier\n')
        clf = joblib.load(clf_filename)
    else:
        clf = LinearSVC(C=1)#, penalty='l1', dual=False)
        # clf = LinearSVC(C=1, penalty='l1', dual=False)
        clf.fit(X_train, y_train)
        joblib.dump(clf, clf_filename)
        
    report_file.write('{} total features\n'.format(len(clf.coef_[0])))
    
    report_file.write('Top 100 positive discriminative features:\n')
    report_file.flush()
    coef_df = pd.DataFrame({'coefficients': clf.coef_[0], 'words': tfidf_vectorizer.get_feature_names()})
    report_file.write('{}\n'.format(coef_df.sort('coefficients', ascending=False).head(100)))
    report_file.write('\nTop 100 negative discriminative features:\n')
    report_file.write('{}\n'.format(coef_df.sort('coefficients').head(100)))
    report_file.write('\n')
        
    # training stats
    decs = clf.decision_function(X_train)
    train_ids = positive_train_ids + negative_train_ids
    report_file.write('{}\t{}\t{}\n'.format(len(train_ids), len(decs), len(y_train)))
    report_file.flush()
    results_df = pd.DataFrame({'id': train_ids, 'decision': decs, 'true_label': y_train})
    print_result_statistics(results_df, report_file, positive_train_user_posts, negative_train_user_posts)

    # testing stats
    decs = clf.decision_function(X_test)
    test_ids = positive_test_ids + negative_test_ids
    report_file.write('{}\t{}\t{}\n'.format(len(test_ids), len(decs), len(y_test)))
    report_file.flush()
    results_df = pd.DataFrame({'id': test_ids, 'decision': decs, 'true_label': y_test})    
    print_result_statistics(results_df, report_file, positive_test_user_posts, negative_test_user_posts)

    report_file.close()