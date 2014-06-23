from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.decomposition import PCA
from sklearn.decomposition import TruncatedSVD
from sklearn.cross_validation import train_test_split
from sklearn.cross_validation import KFold
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.linear_model import SGDRegressor
from sklearn.linear_model import Ridge
from sklearn.linear_model import RidgeCV
from sklearn.linear_model import Lasso
from sklearn.random_projection import SparseRandomProjection
from sklearn.tree import DecisionTreeRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.grid_search import GridSearchCV
from sklearn import metrics
from scipy.sparse import csc_matrix
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
cached_filenames = True
cached_users = True
cached_features = True
cached_tsvd = True
input = 'filename' # will pass a list of filenames to fit that vectorizer will read
min_df = 0.01 # minimum number/proportion of documents containing word
max_df = 0.9 # maximum number/proportion of documents containing word
tokenizer = happiestfuntokenizing.Tokenizer().tokenize # custom tokenizer
ngram_range = (1, 3) # range of lengths for n-grams

user_sample_file = '/data/user_samples/user_sample_50000_with_birth_year_and_gender.tsv'
data_dir = '/data/user_documents/all_originating_posts'
cache_dir = '/data/caches'

test_size = 0.2
num_folds = 5
num_truncated_features = 1000

if len(sys.argv) > 1:
    outcome = sys.argv[1]
else:
    outcome = 'age'
    # outcome = 'gender'
    
if len(sys.argv) > 2:
    sample_size = int(sys.argv[2])
else:
    sample_size = 1000
    
random_state = 42

# models is a list of estimator class, optional parameters, and sparsity flag
if outcome == 'age':
    score_type = 'continuous'
    models = []
    
    # grid search for ridge regression over a range of alpha penalties
    alphas = [x/100.0 for x in range(5, 201, 5)]
    models.append( (GridSearchCV, {'estimator': Ridge(), 
                                   'param_grid': [{'alpha': alphas}], 
                                   'cv': 5, 
                                   'scoring': 'r2',
                                   'verbose': 3}, True, True) ) 

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
print('Building user to birth year and gender mapping')
user_to_birth_year_and_gender = {}
for user_filename in [user_sample_file]:
    user_file = open(user_filename, 'r')
    for line in user_file:
        vals = line.strip().split('\t')
        user_to_birth_year_and_gender[vals[0]] = (int(vals[1]), vals[3])
    user_file.close()

print('Building list of user-message-document filenames')
byte_size_minimum = 1000 # default: 5000
min_birth_year = 1949# default: 1949
max_birth_year = 2014# default: 2001
filenames = []
y = []

if not cached_filenames and not cached_users:
    all_documents = os.listdir(data_dir)
    random.shuffle(all_documents)
    for document in all_documents:
        if len(filenames) >= sample_size:
            break
        user_id = document.split('_')[0]
        if user_id in user_to_birth_year_and_gender \
            and max_birth_year >= user_to_birth_year_and_gender[user_id][0] >= min_birth_year:
            filename = data_dir + '/' + document
            if os.stat(filename).st_size > byte_size_minimum:
                filenames.append(filename)
                if outcome == 'age':
                    y.append(user_to_birth_year_and_gender[user_id][0])
                else:
                    y.append(user_to_birth_year_and_gender[user_id][1])
    y = np.array(y)
    filenames = np.array(filenames)
    joblib.dump(y, cache_dir + '/' + 'users_{}_{}_cache.out'.format(sample_size, outcome))
    joblib.dump(filenames, cache_dir + '/' + 'filenames_{}_cache.out'.format(sample_size))
elif cached_filenames and not cached_users:
    filenames = joblib.load(cache_dir + '/' + 'filenames_{}_cache.out'.format(sample_size))
    for filename in filenames:
        user_id = filename.split('/')[-1].split('_')[0]
        if outcome == 'age':
            y.append(user_to_birth_year_and_gender[user_id][0])
        else:
            y.append(user_to_birth_year_and_gender[user_id][1])
    y = np.array(y)
    joblib.dump(y, cache_dir + '/' + 'users_{}_{}_cache.out'.format(sample_size, outcome))
else:
    y = joblib.load(cache_dir + '/' + 'users_{}_{}_cache.out'.format(sample_size, outcome))
    filenames = joblib.load(cache_dir + '/' + 'filenames_{}_cache.out'.format(sample_size))
    
print('...found {} documents with more than {} bytes'.format(len(filenames), byte_size_minimum))

if not cached_features and not cached_tsvd:
    print('Creating document x term counts matrix')
    t_cache_write_start = time()

    # initialize the vectorizer with the given parameters
    vectorizer = CountVectorizer(input=input, min_df=min_df, max_df=max_df,
                                       tokenizer=tokenizer, ngram_range=ngram_range)

    # read in, tokenize, and create a sparse matrix of document x term counts
    X = vectorizer.fit_transform(filenames)
 
    # transform the matrix of term counts into a matrix of tfidfs
    tfidf_transformer = TfidfTransformer()
    print('Transforming with tfidf')
    X = tfidf_transformer.fit_transform(X)
    
    # output tfidf matrix to file
    mmwrite(cache_dir + '/' + 'user_messages_{}_1kB_age_restricted_tfidf.mtx'.format(sample_size), X)
    print('\tdone. time: {}'.format(time() - t_cache_write_start))
elif cached_features and not cached_tsvd:
    print('Reading in cached features...')
    t_cache_read_start = time()
    X = mmread(cache_dir + '/' + 'user_messages_{}_1kB_age_restricted_tfidf.mtx'.format(sample_size))
    X = csc_matrix(X)
    print(X.shape)
    print('\tdone. time: {}'.format(time() - t_cache_read_start))

# print('Sparse random projection...')
# t_srp_start = time()
# srp = SparseRandomProjection()
# X = srp.fit_transform(X)
# print(X.shape)
# print('\tdone. time: {}'.format(time() - t_srp_start))
# print('PCA...')
# t_pca_start = time()
# pca = PCA(n_components=num_truncated_features)
# X = pca.fit_transform(X.todense())
# X = csc_matrix(X)
# print(X.shape)
# print('\tdone. time: {}'.format(time() - t_pca_start))

if not cached_tsvd:
    sys.stdout.write('TruncatedSVD...\n')
    t_tsvd_start = time()
    t_svd = TruncatedSVD(n_components=num_truncated_features)
    X = t_svd.fit_transform(X)
    X = csc_matrix(X)
    sys.stdout.write('{}\n'.format(X.shape))
    mmwrite(cache_dir + '/' + 'user_messages_{}_1kB_age_restricted_tfidf_{}_tsvd.mtx'.format(sample_size, num_truncated_features), X)
    sys.stdout.write('\tdone. time: {}\n'.format(time() - t_tsvd_start))
    sys.stdout.flush()
else:
    print('Reading in cached truncated features...')
    t_cache_read_start = time()
    X = mmread(cache_dir + '/' + 'user_messages_{}_1kB_age_restricted_tfidf_{}_tsvd.mtx'.format(sample_size, num_truncated_features))
    X = csc_matrix(X)
    print(X.shape)
    print('\tdone. time: {}'.format(time() - t_cache_read_start))

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

print('Training classifier')

for model in models:
    clf_description, clf_performance = benchmark(model[0], model[1], model[2], model[3], score_type)
    sys.stdout.write('{}\t{}\n'.format(clf_description, clf_performance))
    sys.stdout.write('Sample size: {}\nNum folds: {}\n'.format(sample_size, num_folds))
    sys.stdout.write('\t{}\n'.format('\n\t'.join(['{}: {}'.format(k, v) for k, v in 
                                                    mean_results(clf_performance).items()])))
    sys.stdout.flush()

print('\tdone. total time: {}'.format(time() - t0))