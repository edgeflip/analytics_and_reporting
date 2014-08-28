from sklearn_prediction_utils import models_report
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
    
# feature_classes = {1: 'user_posts', 
#                    2: 'from_friend_posts',
#                    3: 'user_links'}
feature_classes = {3: 'user_links'}

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
    features[feature_idx] = csc_matrix(mmread(cache_dir + '/' + 'topic_proportions_{}_{}_1000-max.mtx'.format(feature_classes[feature_idx], sample_size)))

X = hstack([feature_matrix for feature_matrix in features.values()])
sys.stdout.write('{}\n'.format(X.shape))
sys.stdout.flush()

sys.stdout.write('Training classifiers\...n')
models_report(models, X, y, sample_size, score_type)
sys.stdout.write('\tdone. total time: {}\n'.format(time() - t0))