from sklearn.cross_validation import KFold
from sklearn.linear_model import Ridge
# from sklearn.linear_model import Lasso
from sklearn.svm import LinearSVC
from sklearn.grid_search import GridSearchCV
from sklearn import metrics
from scipy.sparse import csc_matrix, hstack
from scipy.io import mmread
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
    outcome = 'age' # 'gender'
    
if len(sys.argv) > 2:
    sample_size = int(sys.argv[2])
else:
    sample_size = 1000
    
feature_classes = {1: 'user_posts', 
                   2: 'from_friend_posts'}
cache_dir = '/data/caches/user_and_from_friend_posts'

test_size = 0.2
num_folds = 5
min_birth_year = 1949
max_birth_year = 2014
random_state = 42

# models is a list of estimator class, optional parameters, and sparsity flag
if outcome == 'age':
    score_type = 'continuous'
    models = []
    
    # grid search for ridge regression over a range of alpha penalties
    # NB: copy_X = False 'may' be a problem (documentation says it 'may be overwritten')
#     models.append( (Ridge, {'alpha': 1.45}, True, False) )
    alphas = [x/100.0 for x in range(5, 201, 5)]
    models.append( (GridSearchCV, {'estimator': Ridge(copy_X=False), 
                                   'param_grid': [{'alpha': alphas}], 
                                   'cv': 5, 
                                   'scoring': 'r2',
                                   'verbose': 3}, True, True) ) 
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
#####################

user_ids = joblib.load(cache_dir + '/' + 'users_{}_cache.out'.format(sample_size)) 
y = joblib.load(cache_dir + '/' + 'outcome_{}_{}_cache.out'.format(sample_size, outcome))
sys.stdout.write('{}\n'.format(y.shape))

features = {}
for feature_idx in feature_classes:
    features[feature_idx] = csc_matrix(mmread(cache_dir + '/' + 'topic_proportions_{}_{}_1000.mtx'.format(feature_classes[feature_idx], sample_size)))

X = hstack([feature_matrix for feature_matrix in features.values()])
sys.stdout.write('{}\n'.format(X.shape))
sys.stdout.flush()

######## classifiers ##########

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