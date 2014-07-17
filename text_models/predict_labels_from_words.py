from evaluate_user_rankings import *
from sklearn.grid_search import GridSearchCV
from sklearn.svm import LinearSVC
from scipy.io import mmwrite
import pandas as pd
import os

def get_vectorizer(vocabulary_filename, idf_filename, user_posts_list, negative_train_vector_matrix_filename):
    if os.path.isfile(vocabulary_filename) and os.path.isfile(idf_filename):
        sys.stdout.write('\tReading in cached tfidf vectorizer and negative train vector matrix\n')
        vocab = joblib.load(vocabulary_filename)
        idf = joblib.load(idf_filename)
        tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_df=0.5, min_df=60, vocabulary=vocab)
        n_features = idf.shape[0]
        tfidf_vectorizer._tfidf._idf_diag = spdiags(idf, diags=0, m=n_features, n=n_features)
        return tfidf_vectorizer
    else:
        # No vectorizer cached, fit a new one to the given user-posts and cache their canonical vector
        tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_df=0.5, min_df=60, stop_words=['id', 'name'])
        mx = tfidf_vectorizer.fit_transform([user_posts.posts for user_posts in user_posts_list])
        mmwrite(negative_train_vector_matrix_filename, mx)
        joblib.dump(tfidf_vectorizer.vocabulary_, vocabulary_filename)
        joblib.dump(tfidf_vectorizer.idf_, idf_filename)
        return tfidf_vectorizer

def prune_ids_and_posts_by_word_threshold(ids, user_posts, word_threshold, label):
    temp_ids_pruned = []
    temp_user_posts_pruned = []
    for user_id, user_post in zip(ids, user_posts):
        if len(user_post.posts) >= word_threshold:
            temp_ids_pruned.append(user_id)
            temp_user_posts_pruned.append(user_post)
    sys.stdout.write('\tremoved {} {}\n'.format(len(ids) - len(temp_ids_pruned), label))
    ids = temp_ids_pruned[:]
    user_posts = temp_user_posts_pruned[:]
    return ids, user_posts

if __name__ == '__main__':
    label = sys.argv[1]
    # label = 'vegan'
    word_threshold = None
    average_dir = '/data/user_documents/individual_posts_100000'
    label_dir = '/data/user_documents/individual_posts_{}'.format(label)
    model_run_suffix = '_post_aboutme_words'
    model_run_dir = '/data/model_runs/{}{}'.format(label, model_run_suffix)
    
    # Get positive train/test user ids
    sys.stdout.write('Getting positive training and test ids and posts\n')
    positive_train_ids_filename = label_dir + '/' + 'train-user-ids.txt'
    positive_train_ids = get_ids_from_file(positive_train_ids_filename)
    positive_test_ids_filename = label_dir + '/' + 'test-user-ids.txt'
    positive_test_ids = get_ids_from_file(positive_test_ids_filename)
    
    # Get negative train/test user ids
    sys.stdout.write('Getting negative training and test ids and posts\n')
    negative_train_ids_filename = average_dir + '/' + 'train-user-ids.txt'
    negative_train_ids = get_ids_from_file(negative_train_ids_filename)
    negative_test_ids_filename = average_dir + '/' + 'test-user-ids.txt'
    negative_test_ids = get_ids_from_file(negative_test_ids_filename)
    
    # Remove any positive id that shows up in the negative ids
    positive_ids = set(positive_train_ids) | set(positive_test_ids)
    train_overlap = len(set(negative_train_ids) & positive_ids)
    test_overlap = len(set(negative_test_ids) & positive_ids)
    sys.stdout.write('\tRemoved {} and {} negative ids that appeared in positive list\n'.format(train_overlap, test_overlap))
    negative_train_ids = list(set(negative_train_ids) - positive_ids)
    negative_test_ids = list(set(negative_test_ids) - positive_ids)
    
    # Get positive train/test user posts
    sys.stdout.write('Getting positive training and test posts\n')
    positive_post_filename = label_dir + '/' + 'all-individual-posts.txt'
    positive_aboutme_filename = label_dir + '/' + 'all-individual-aboutme.txt'
    positive_train_user_posts, positive_test_user_posts = get_user_posts([positive_post_filename, 
                                                                          positive_aboutme_filename],
                                                                         set(positive_train_ids),
                                                                         set(positive_test_ids))
    positive_train_user_posts = order_user_posts(positive_train_user_posts, positive_train_ids)
    positive_test_user_posts = order_user_posts(positive_test_user_posts, positive_test_ids)
    
    # Get negative train/test user posts
    sys.stdout.write('Getting negative training and test posts\n')    
    negative_post_filename = average_dir + '/' + 'all-individual-posts.txt'
    negative_aboutme_filename = label_dir + '/' + 'all-individual-aboutme.txt'
    negative_train_user_posts, negative_test_user_posts = get_user_posts([negative_post_filename,
                                                                          negative_aboutme_filename],
                                                                         set(negative_train_ids),
                                                                         set(negative_test_ids))
    negative_train_user_posts = order_user_posts(negative_train_user_posts, negative_train_ids)
    negative_test_user_posts = order_user_posts(negative_test_user_posts, negative_test_ids)
    
    # Remove ids and posts where number of words < threshold
    if word_threshold:
        sys.stdout.write('Pruning ids/posts based on word threshold\n')
        word_threshold = 1000
        negative_train_ids, negative_train_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            negative_train_ids, 
                                                            negative_train_user_posts,
                                                            word_threshold, 
                                                            'negative train')
        negative_test_ids, negative_test_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            negative_test_ids, 
                                                            negative_test_user_posts, 
                                                            word_threshold,
                                                            'negative test')
        positive_train_ids, positive_train_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            positive_train_ids, 
                                                            positive_train_user_posts,
                                                            word_threshold, 
                                                            'positive train')
        positive_test_ids, positive_test_user_posts = prune_ids_and_posts_by_word_threshold(
                                                            positive_test_ids, 
                                                            positive_test_user_posts, 
                                                            word_threshold,
                                                            'positive test')
    
    # Load in tfidf vectorizer    
    sys.stdout.write('Getting Tfidf Vectorizer trained on negative posts\n')
    vocabulary_filename = model_run_dir + '/' + 'tfidf_vocabulary{}.out'.format(model_run_suffix)
    idf_filename = model_run_dir + '/' + 'idf_vector{}.out'.format(model_run_suffix)
    negative_train_vector_matrix_filename = model_run_dir + '/' + 'negative_train_vector_matrix{}.mtx'.format(model_run_suffix)
    tfidf_vectorizer = get_vectorizer(vocabulary_filename, idf_filename, 
                                      negative_train_user_posts, 
                                      negative_train_vector_matrix_filename)
    
    # Build word vectors for all of them
    sys.stdout.write('Getting vectors for positive train posts\n')
    positive_train_vector_matrix_filename = model_run_dir + '/' + 'positive_train_vector_matrix{}.mtx'.format(model_run_suffix)
    positive_train_vector_mx = get_post_vectors(tfidf_vectorizer, positive_train_user_posts, 
                                               positive_train_vector_matrix_filename)
    sys.stdout.write('Getting vectors for positive test posts\n')
    positive_test_vector_matrix_filename = model_run_dir + '/' + 'positive_test_vector_matrix{}.mtx'.format(model_run_suffix)
    positive_test_vector_mx = get_post_vectors(tfidf_vectorizer, positive_test_user_posts, 
                                               positive_test_vector_matrix_filename)
    
    sys.stdout.write('Getting vectors for negative train posts\n')    
    negative_train_vector_mx = get_post_vectors(tfidf_vectorizer, negative_train_user_posts, 
                                               negative_train_vector_matrix_filename)
    
    sys.stdout.write('Getting vectors for negative test posts\n')
    negative_test_vector_matrix_filename = model_run_dir + '/' + 'negative_test_vector_matrix{}.mtx'.format(model_run_suffix)
    negative_test_vector_mx = get_post_vectors(tfidf_vectorizer, negative_test_user_posts, 
                                               negative_test_vector_matrix_filename)
    
    # Stack positive/negative for train/test
    sys.stdout.write('Stacking data\n')    
    X_train = vstack([positive_train_vector_mx, negative_train_vector_mx])
    X_test = vstack([positive_test_vector_mx, negative_test_vector_mx])
    y_train = np.concatenate((np.repeat([1], positive_train_vector_mx.shape[0]), 
                              np.repeat([0], negative_train_vector_mx.shape[0])))
    y_test = np.concatenate((np.repeat([1], positive_test_vector_mx.shape[0]), 
                              np.repeat([0], negative_test_vector_mx.shape[0])))
    
    sys.stdout.write('Training classifier\n')
    clf_filename = model_run_dir + '/' + 'linear_svc{}.out'.format(model_run_suffix)
    if os.path.isfile(clf_filename):
        sys.stdout.write('\tReading in cached classifier\n')
        clf = joblib.load(clf_filename)
    else:
        clf = LinearSVC(C=1.0)
        clf.fit(X_train, y_train)
        joblib.dump(clf, clf_filename)
    
    sys.stdout.write('Top 10 positive discriminative features:\n')
    coef_df = pd.DataFrame({'coefficients': clf.coef_[0], 'words': tfidf_vectorizer.get_feature_names()})
    sys.stdout.write('{}\n'.format(coef_df.sort('coefficients', ascending=False).head(10)))
    sys.stdout.write('\nTop 10 negative discriminative features:\n')
    sys.stdout.write('{}\n'.format(coef_df.sort('coefficients').head(10)))
    sys.stdout.write('\n')
    
    decs = clf.decision_function(X_test)
    test_ids = positive_test_ids + negative_test_ids

    results_df = pd.DataFrame({'id': test_ids, 'decision': decs, 'true_label': y_test})

    tok = re.compile(r'(?u)\b\w\w+\b')
    pos_words = [tok.findall(up.posts.lower()) for up in positive_test_user_posts]
    neg_words = [tok.findall(up.posts.lower()) for up in negative_test_user_posts]
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

    sys.stdout.write('             Accuracy: {}\n'.format(1.0*(num_tp+num_tn) / len(results_df)))
    sys.stdout.write('   True positive rate: {}\n'.format(1.0*num_tp / (num_tp+num_fn)))
    sys.stdout.write('   True negative rate: {}\n'.format(1.0*num_tn / (num_tn+num_fp)))
    sys.stdout.write('  False positive rate: {}\n'.format(1.0*num_fp / (num_fp+num_tn)))
    sys.stdout.write('  False negative rate: {}\n'.format(1.0*num_fn / (num_fn+num_tp)))
    sys.stdout.write(' False discovery rate: {}\n'.format(1.0*num_fp / (num_fp+num_tp)))
    sys.stdout.write('            Precision: {}\n'.format(1.0*num_tp / (num_fp+num_tp)))
    sys.stdout.write('               Recall: {}\n'.format(1.0*num_tp / (num_fn+num_tp)))
    sys.stdout.write('Diagnostic odds ratio: {}\n'.format((1.0*num_tp/num_fn) / (1.0*num_fp/num_tn)))
    sys.stdout.write('\n')
    
    # get count, mean score, mean num words for a different cross-sections
    sys.stdout.write('Type of pred:\tN\tmean score\tmean num words\n')
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
        sys.stdout.write('{}: {}\t{}\t{}\n'.format(out_label, c, m, w))
    sys.stdout.write('\n')
    
    # peek at the top positively labeled users that are true positives:
    sys.stdout.write('Top 10 true positives\n')
    sys.stdout.write('{}\n'.format(results_df[pos_true_condition & pos_pred_condition].sort(
                                    'decision', ascending=False)[:10][['decision', 'id']]))
    sys.stdout.write('\n')
    
    # and false positives:
    sys.stdout.write('Top 10 false positives\n')
    sys.stdout.write('{}\n'.format(results_df[neg_true_condition & pos_pred_condition].sort(
                                    'decision', ascending=False)[:10][['decision', 'id']]))