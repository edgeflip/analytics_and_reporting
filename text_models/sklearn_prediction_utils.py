from sklearn.cross_validation import KFold
from sklearn import metrics
import numpy as np
import sys
from time import time

num_folds = 5
random_state = 42

def benchmark(X, y, clf_class, params, sparse_flag, grid_flag, sample_size, score_type):    
    clf = clf_class(**params)
    
    res_scores = {}
    for train_idxs, test_idxs in KFold(n=sample_size, n_folds=num_folds, 
                                       random_state=random_state, shuffle=True):
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
    
def get_predictions_across_folds(X, y, clf_class, params, sparse_flag, sample_size):    
    clf = clf_class(**params)
    
    all_predictions = np.empty_like(y)
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
    
        t_test_classifier_start = time()
        if sparse_flag:
            preds = clf.predict(X[test_idxs])
        else:
            preds = clf.predict(X[test_idxs].todense())
        test_time = time() - t_test_classifier_start
        
        all_predictions[test_idxs] = preds
    return all_predictions

def mean_results(results):
    '''
    results should be a dictionary of score_name: [value1, ...]
    '''
    scores_to_mean_values = {}
    for score_name, value_list in results.items():
        scores_to_mean_values[score_name] = 1.0*sum(value_list)/len(value_list)
    return scores_to_mean_values
    
def models_report(models, X, y, sample_size, score_type):
    for model in models:
        clf_description, clf_performance = benchmark(X, y, model[0], model[1], model[2], model[3], sample_size, score_type)
        sys.stdout.write('{}\t{}\n'.format(clf_description, clf_performance))
        sys.stdout.write('Sample size: {}\nNum folds: {}\n'.format(sample_size, num_folds))
        sys.stdout.write('\t{}\n'.format('\n\t'.join(['{}: {}'.format(k, v) for k, v in 
                                                        mean_results(clf_performance).items()])))
        sys.stdout.flush()
