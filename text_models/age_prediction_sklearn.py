from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
# from sklearn.decomposition import PCA
from sklearn.decomposition import TruncatedSVD
# from sklearn.cross_validation import train_test_split
from sklearn.cross_validation import KFold
# from sklearn.naive_bayes import MultinomialNB
# from sklearn.linear_model import SGDClassifier
# from sklearn.linear_model import SGDRegressor
from sklearn.linear_model import Ridge
# from sklearn.linear_model import RidgeCV
# from sklearn.linear_model import Lasso
# from sklearn.random_projection import SparseRandomProjection
# from sklearn.tree import DecisionTreeRegressor
# from sklearn.neighbors import KNeighborsRegressor
# from sklearn.svm import SVC
from sklearn.svm import LinearSVC
# from sklearn.pipeline import Pipeline
from sklearn.grid_search import GridSearchCV
from sklearn import metrics
from scipy.sparse import csc_matrix, hstack
from scipy.io import mmwrite, mmread
import happiestfuntokenizing
import numpy as np
import joblib
from time import time
import random
import sys
import os

t0 = time()

#### parameters ####
if len(sys.argv) > 1:
    outcome = sys.argv[1]
else:
    outcome = 'age'
    # outcome = 'gender'
    
if len(sys.argv) > 2:
    sample_size = int(sys.argv[2])
else:
    sample_size = 1000

feature_classes = {1: 'user_posts', 
                   2: 'from_friend_posts'}
cached_filenames = {1: True, 2: True}
cached_users = True
cached_outcome = {'age': True, 'gender': False}
cached_features = {1: True, 2: True}
cached_tsvd = {1: True, 2: True}

input = 'filename' # will pass a list of filenames to fit that vectorizer will read
min_df = 0.01 # minimum number/proportion of documents containing word
max_df = 0.9 # maximum number/proportion of documents containing word
tokenizer = happiestfuntokenizing.Tokenizer().tokenize # custom tokenizer
ngram_range = (1, 3) # range of lengths for n-grams

user_sample_file = '/data/user_samples/user_sample_50000_with_birth_year_and_gender.tsv'
data_dirs = {1: '/data/user_documents/all_originating_posts', 
             2: '/data/user_documents/all_from_friend_posts'}
document_suffixes = {1: '_messages.txt',
                     2: '_from_friend_messages.txt'}
cache_dir = '/data/caches/user_and_from_friend_posts'

test_size = 0.2
num_folds = 5
num_truncated_features = 1000
random_state = 42
document_byte_size_minimum = 1#1000
min_birth_year = 1949
max_birth_year = 2014

# models is a list of estimator class, optional parameters, and sparsity flag
if outcome == 'age':
    score_type = 'continuous'
    models = []
    
    # grid search for ridge regression over a range of alpha penalties
    # NB: copy_X = False 'may' be a problem (documentation says it 'may be overwritten')
    models.append( (Ridge, {'alpha': 1.45}, True, False) )
#     alphas = [x/100.0 for x in range(5, 201, 5)]
#     models.append( (GridSearchCV, {'estimator': Ridge(copy_X=False), 
#                                    'param_grid': [{'alpha': alphas}], 
#                                    'cv': 5, 
#                                    'scoring': 'r2',
#                                    'verbose': 3}, True, True) ) 

    # Too slow with 1e6 n_iter and too inaccurate with fewer iterations
    # stochastic gradient descent using L1 and L2 penalties
#     models.extend( [(SGDRegressor, {'penalty': 'l2', 'n_iter': 1e6, 'shuffle': True}, True, False), 
#                     (SGDRegressor, {'penalty': 'l1', 'n_iter': 1e6, 'shuffle': True}, True, False)] )
#     
#     # grid search for stochastic gradient descent over L1 ratios for elasticnet penalty
#     l1_ratios = [x/100.0 for x in range(5, 96, 5)]
#     models.append( (GridSearchCV, {'estimator': SGDRegressor(), 
#                                    'param_grid': [{'l1_ratio': l1_ratios, 
#                                                    'penalty': ['elasticnet'],
#                                                    'n_iter': [1e6],
#                                                    'shuffle': [True]}], 
#                                    'cv': 3, 
#                                    'scoring': 'r2'}, True, True) ) 
    # (KNeighborsRegressor, {}, True) 
    # (DecisionTreeRegressor, {}, False)
else:
    score_type = 'discrete'
    models = []

    # grid search over linear SVM classifiers
    cs = [1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5]
    models.append( (GridSearchCV, {'estimator': LinearSVC(), 
                                   'param_grid': [{'C': cs}], 
                                   'cv': 5, 
                                   'scoring': 'accuracy',
                                   'verbose': 3}, True, True) ) 
    
    # Too slow with 1e6 n_iter and too inaccurate with fewer iterations
    # stochastic gradient descent using L1 and L2 penalties
#     models.extend( [(SGDClassifier, {'penalty': 'l2', 'n_iter': 1e6, 'shuffle': True}, True, False), 
#                     (SGDClassifier, {'penalty': 'l1', 'n_iter': 1e6, 'shuffle': True}, True, False)] ) 
#     
#     # grid search for stochastic gradient descent over L1 ratios for elasticnet penalty
#     l1_ratios = [x/100.0 for x in range(5, 96, 5)]
#     models.append( (GridSearchCV, {'estimator': SGDClassifier(), 
#                                    'param_grid': [{'l1_ratio': l1_ratios, 
#                                                    'penalty': ['elasticnet'],
#                                                    'n_iter': 1e6,
#                                                    'shuffle': True}], 
#                                    'cv': 3, 
#                                    'scoring': 'r2'}, True, True) ) 
                
#                  (SVC, {'kernel': 'rbf', 'C': 1.0, 'gamma': 1e-3}, True), 
#                  (SVC, {'kernel': 'rbf', 'C': 1.0, 'gamma': 1e-4}, True),  
#                  (SVC, {'kernel': 'rbf', 'C': 10.0, 'gamma': 1e-3}, True), 
#                  (SVC, {'kernel': 'rbf', 'C': 10.0, 'gamma': 1e-4}, True), 
#                  (SVC, {'kernel': 'rbf', 'C': 100.0, 'gamma': 1e-3}, True), 
#                  (SVC, {'kernel': 'rbf', 'C': 100.0, 'gamma': 1e-4}, True)
#####################

# build up list of filenames that will be input to vectorizer (documents->features)
# also construct an array of class labels to align with the document features
user_to_birth_year_and_gender = {}
for user_filename in [user_sample_file]:
    user_file = open(user_filename, 'r')
    for line in user_file:
        vals = line.strip().split('\t')
        user_to_birth_year_and_gender[vals[0]] = (int(vals[1]), vals[3])
    user_file.close()

# Load list of user ids
sys.stdout.write('Getting user ids...\n')
user_ids = []
if not cached_users:
    # Build list of possible users by age restriction and existence of a document in every
    # feature class
    # todo: could extend this to more complex combinations, like existence of at least one 
    #       feature class or class-specific byte minimums

    # build dict of sets of documents for each feature class
    feature_documents = {feature_idx: set(os.listdir(data_dirs[feature_idx])) 
                            for feature_idx in feature_classes}
    possible_user_ids = []
    for user_id, (birth_year, gender) in user_to_birth_year_and_gender.items():
        if max_birth_year >= birth_year >= min_birth_year: # passes age filter
            if all(['{}{}'.format(user_id, document_suffixes[feature_idx]) in feature_documents[feature_idx]
                        for feature_idx in feature_classes]): # passes feature document filter
                possible_user_ids.append(user_id)

#     if os.stat(filename).st_size > document_byte_size_minimum: # filter for document size
    random.shuffle(possible_user_ids)
    user_ids = np.array(possible_user_ids[:sample_size])
    joblib.dump(user_ids, cache_dir + '/' + 'users_{}_cache.out'.format(sample_size))
else:
    user_ids = joblib.load(cache_dir + '/' + 'users_{}_cache.out'.format(sample_size))    

# Load outcome values
sys.stdout.write('Getting outcome values for {}...\n'.format(outcome))
y = []
if not cached_outcome[outcome]:
    for user_id in user_ids:
        if outcome == 'age':
            y.append(user_to_birth_year_and_gender[user_id][0])
        else:
            y.append(user_to_birth_year_and_gender[user_id][1])
    y = np.array(y)
    joblib.dump(y, cache_dir + '/' + 'outcome_{}_{}_cache.out'.format(sample_size, outcome))
else:
    y = joblib.load(cache_dir + '/' + 'outcome_{}_{}_cache.out'.format(sample_size, outcome))

# Load filenames for each feature class
sys.stdout.write('Getting filenames for each feature class...\n')
filenames = {}
if not all([is_cached for is_cached in cached_filenames.values()]):
    for user_id in user_ids:
        for feature_idx in feature_classes:
            if not cached_filenames[feature_idx]:
                filename = data_dirs[feature_idx] + '/' + '{}{}'.format(user_id, document_suffixes[feature_idx])
                filenames.setdefault(feature_idx, []).append(filename)
    for feature_idx in feature_classes:
        if not cached_filenames[feature_idx]:
            filenames[feature_idx] = np.array(filenames[feature_idx])
            joblib.dump(filenames[feature_idx], cache_dir + '/' + 'filenames_{}_{}_cache.out'.format(sample_size, feature_classes[feature_idx]))
for feature_idx in feature_classes:
    if cached_filenames[feature_idx]:
        filenames[feature_idx] = joblib.load(cache_dir + '/' + 'filenames_{}_{}_cache.out'.format(sample_size, feature_classes[feature_idx]))

# Load in or build tfidf matrix or truncated SVD matrix if cached
features = {feature_idx: None for feature_idx in feature_classes} # dict from feature_idx to feature matrix
for feature_idx in feature_classes:
    if not cached_features[feature_idx] and not cached_tsvd[feature_idx]:
        sys.stdout.write('Creating document x term counts matrix for {}\n'.format(feature_classes[feature_idx]))
        t_cache_write_start = time()

        # initialize the vectorizer with the given parameters
        vectorizer = CountVectorizer(input=input, min_df=min_df, max_df=max_df,
                                           tokenizer=tokenizer, ngram_range=ngram_range)

        # read in, tokenize, and create a sparse matrix of document x term counts
        features[feature_idx] = vectorizer.fit_transform(filenames[feature_idx])
 
        # transform the matrix of term counts into a matrix of tfidfs
        tfidf_transformer = TfidfTransformer()
        sys.stdout.write('Transforming with tfidf for {}\n'.format(feature_classes[feature_idx]))
        features[feature_idx] = tfidf_transformer.fit_transform(features[feature_idx])
    
        # output tfidf matrix to file
        mmwrite(cache_dir + '/' + 'tfidf_{}_{}.mtx'.format(sample_size, feature_classes[feature_idx]), features[feature_idx])
        sys.stdout.write('\tdone. time: {}\n'.format(time() - t_cache_write_start))
        sys.stdout.flush()
    elif cached_features[feature_idx] and not cached_tsvd[feature_idx]:
        sys.stdout.write('Reading in cached features for {}...\n'.format(feature_classes[feature_idx]))
        t_cache_read_start = time()
        features[feature_idx] = mmread(cache_dir + '/' + 'tfidf_{}_{}.mtx'.format(sample_size, feature_classes[feature_idx]))
        features[feature_idx] = csc_matrix(features[feature_idx])
        sys.stdout.write('{}\n'.format(features[feature_idx].shape))
        sys.stdout.write('\tdone. time: {}\n'.format(time() - t_cache_read_start))
        sys.stdout.flush()

    if not cached_tsvd[feature_idx]:
        sys.stdout.write('Running TruncatedSVD for {}...\n'.format(feature_classes[feature_idx]))
        t_tsvd_start = time()
        t_svd = TruncatedSVD(n_components=num_truncated_features)
        features[feature_idx] = t_svd.fit_transform(features[feature_idx])
        features[feature_idx] = csc_matrix(features[feature_idx])
        sys.stdout.write('{}\n'.format(features[feature_idx].shape))
        mmwrite(cache_dir + '/' + 'tsvd_{}_{}_{}.mtx'.format(sample_size, feature_classes[feature_idx], num_truncated_features), features[feature_idx])
        sys.stdout.write('\tdone. time: {}\n'.format(time() - t_tsvd_start))
        sys.stdout.flush()
    else:
        sys.stdout.write('Reading in cached truncated features for {}...\n'.format(feature_classes[feature_idx]))
        t_cache_read_start = time()
        features[feature_idx] = mmread(cache_dir + '/' + 'tsvd_{}_{}_{}.mtx'.format(sample_size, feature_classes[feature_idx], num_truncated_features))
        features[feature_idx] = csc_matrix(features[feature_idx])
        sys.stdout.write('{}\n'.format(features[feature_idx].shape))
        sys.stdout.write('\tdone. time: {}\n'.format(time() - t_cache_read_start))
        sys.stdout.flush()

# hstack together all features
sys.stdout.write('Stacking feature classes together...\n')
X = hstack([feature_matrix for feature_matrix in features.values()])
sys.stdout.write('{}\n'.format(X.shape))
sys.stdout.flush()

# sys.exit()

######## classifiers ##########

# pipeline = Pipeline([
#         ('count_vectorizer', CountVectorizer(input=input, min_df=min_df, 
#                                    tokenizer=tokenizer, ngram_range=ngram_range)),
#         ('tfidf_transformer', TfidfTransformer())])
# 
# X = pipeline.transform(filenames)

# print('Splitting into train/test sets, {}/{}'.format(int((1-test_size)*100), int(test_size*100)))
# t_train_test_split_start = time()
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
# print('\tdone. time: {}'.format(time() - t_train_test_split_start))

def mean_results(results):
    '''
    results should be a dictionary of score_name: [value1, ...]
    '''
    scores_to_mean_values = {}
    for score_name, value_list in results.items():
        scores_to_mean_values[score_name] = 1.0*sum(value_list)/len(value_list)
    return scores_to_mean_values
    
def benchmark(clf_class, params, sparse_flag, grid_flag, score_type):    
    clf = clf_class(**params)
    
    res_scores = {}
    for train_idxs, test_idxs in KFold(n=sample_size, n_folds=num_folds, random_state=random_state):
        t_train_classifier_start = time()
        sys.stdout.write('trying to fit\n')
        if sparse_flag:
            clf.fit(X[train_idxs], y[train_idxs])
        else:
            clf.fit(X[train_idxs].todense(), y[train_idxs])
        train_time = time() - t_train_classifier_start
        sys.stdout.write('done fitting\n')
        sys.stdout.flush()
    
        if grid_flag:
            sys.stdout.write('{}\n'.format(clf.best_estimator_))
            sys.stdout.flush()
    
        t_test_classifier_start = time()
        if sparse_flag:
            preds = clf.predict(X[test_idxs])
        else:
            preds = clf.predict(X[test_idxs].todense())
        test_time = time() - t_test_classifier_start
    
        if score_type == 'continuous':
            r2 = metrics.r2_score(y[test_idxs], preds)
            r = np.sqrt(r2)
            mean_abs_err = metrics.mean_absolute_error(y[test_idxs], preds)
            res_scores.setdefault('R2', []).append(r2)
            res_scores.setdefault('R', []).append(r)
            res_scores.setdefault('mean(|err|)', []).append(mean_abs_err)
            res_scores.setdefault('train_time', []).append(train_time)
            res_scores.setdefault('test_time', []).append(test_time)
        else:
            accuracy = metrics.accuracy_score(y[test_idxs], preds)
            res_scores.setdefault('accuracy', []).append(accuracy)
            res_scores.setdefault('train_time', []).append(train_time)
            res_scores.setdefault('test_time', []).append(test_time)
        
    clf_descr = str(clf).split('(')[0] + str(params)
    return clf_descr, res_scores

sys.stdout.write('Training classifiers\...n')

for model in models:
    clf_description, clf_performance = benchmark(model[0], model[1], model[2], model[3], score_type)
    sys.stdout.write('{}\t{}\n'.format(clf_description, clf_performance))
    sys.stdout.write('Sample size: {}\nNum folds: {}\n'.format(sample_size, num_folds))
    sys.stdout.write('\t{}\n'.format('\n\t'.join(['{}: {}'.format(k, v) for k, v in 
                                                    mean_results(clf_performance).items()])))
    sys.stdout.flush()

sys.stdout.write('\tdone. total time: {}\n'.format(time() - t0))