from db_utils import redshift_connect, redshift_disconnect, execute_query
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.spatial.distance import cosine, wminkowski
from scipy.sparse import csr_matrix, vstack, spdiags
from collections import namedtuple
from cStringIO import StringIO
from scipy.io import mmwrite, mmread
from scipy.linalg import norm
import numpy as np
import subprocess
import operator
import joblib
import sys
import re
import os

np.seterr('ignore')
User_Posts = namedtuple('User_Posts', ['fbid', 'posts'])

def get_ids_from_file(ids_filename):
    ids_file = open(ids_filename, 'r')
    ids = [line.strip() for line in ids_file]
    ids_file.close()
    return ids

def get_user_posts(post_document_filenames, *user_sets):
    '''
    Return a list of lists of User_Posts by scanning all posts in post_document_filename
    and recording them according to the position of the user_set that the user falls 
    into (if at all). If no user_sets are supplied, it returns a list of a single 
    list of User_Posts.
    '''
    if not user_sets:
        user_to_posts = [{}]
    else:
        user_to_posts = [{} for i in range(len(user_sets))]
    for post_document_filename in post_document_filenames:
        post_document_file = open(post_document_filename, 'r')
        for line in post_document_file:
            vals = line.split()
            user = vals[0].split('_')[0]
            if not user_sets:
                bucket = 0
            else:
                bucket = -1
                for idx, user_set in enumerate(user_sets):
                    if user in user_set:
                        bucket = idx
                        break
            if bucket != -1:
                words = ' '.join(vals[1:])
                user_to_posts[bucket].setdefault(user, StringIO())
                user_to_posts[bucket][user].write(words)
                user_to_posts[bucket][user].write(' ')
        post_document_file.close()
    return [[User_Posts(user, posts.getvalue()) for user, posts in user_to_posts[bucket].items()] 
                for bucket in range(len(user_to_posts))]

def order_user_posts(user_posts, user_ids):
    '''
    Sort a list of User_Posts by the fixed order occurring in user_ids
    '''
    user_id_to_user_post = {}
    for user_post in user_posts:
        user_id_to_user_post[user_post.fbid] = user_post
    return [user_id_to_user_post[user_id] for user_id in user_ids]

def get_vectorizer(vocabulary_filename, idf_filename, user_posts_list, canonical_word_vector_filename):
    if os.path.isfile(vocabulary_filename) and os.path.isfile(idf_filename):
        sys.stdout.write('\tReading in cached tfidf vectorizer\n')
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
        joblib.dump(np.array(csr_matrix.mean(mx, axis=0)).ravel(), canonical_word_vector_filename)
        joblib.dump(tfidf_vectorizer.vocabulary_, vocabulary_filename)
        joblib.dump(tfidf_vectorizer.idf_, idf_filename)
        return tfidf_vectorizer

def get_canonical_vector(user_posts_list, tfidf_vectorizer, canonical_word_vector_filename):
    if os.path.isfile(canonical_word_vector_filename):
        sys.stdout.write('\tReading in cached canonical word vector\n')
        return joblib.load(canonical_word_vector_filename).ravel()
    else:
        mx = tfidf_vectorizer.transform([user_posts.posts for user_posts in user_posts_list])
        canonical_word_vector = np.array(csr_matrix.mean(mx, axis=0)).ravel()
        joblib.dump(canonical_word_vector, canonical_word_vector_filename)
        return canonical_word_vector

def get_largest_word_differences(vector1, vector2, tfidf_vectorizer, k=10):
    words = tfidf_vectorizer.get_feature_names()
    ratio_dict = {i: ratio for i, ratio in enumerate(vector1/vector2)}
    sorted_ratios = sorted(ratio_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
    print('Top {}:'.format(k))
    for col, ratio in sorted_ratios[:k]:
        print(words[col], ratio)
    print('\nBottom {}:'.format(k))
    bottom_k = sorted_ratios[-k:]
    bottom_k.reverse()
    for col, ratio in bottom_k:
        print(words[col], ratio)

def get_post_vectors(tfidf_vectorizer, user_posts_list, post_vector_matrix_filename):
    if os.path.isfile(post_vector_matrix_filename):
        sys.stdout.write('\tReading in cached post vectors\n')
        return csr_matrix(mmread(post_vector_matrix_filename))
    else:
        vector_mx = tfidf_vectorizer.transform([user_posts.posts for user_posts in user_posts_list])
        mmwrite(post_vector_matrix_filename, vector_mx)
        return vector_mx

def get_users_sorted_by_canonical_distance(user_list, user_to_word_vector_mx, canonical_vector, 
                                           distance=cosine, distance_kwargs={}):
    '''
    For each row in the user_to_word_vector matrix, compute the distance to canonical_vector
    and return list of sorted user-distance tuples
    '''
    user_to_distance = {user: distance(user_to_word_vector_mx[user_idx].toarray()[0], 
                                       canonical_vector, **distance_kwargs)
                            for user_idx, user in enumerate(user_list)}    
    sorted_user_distances = sorted(user_to_distance.iteritems(), key=operator.itemgetter(1), reverse=False)
    return sorted_user_distances

def cosine_threshold_distance(vec1, vec2, weights, percentile):
    threshold = np.percentile(weights, percentile)
    distance = cosine(vec1[weights >= threshold], vec2[weights >= threshold])
    return distance if not np.isnan(distance) else 1.0

def cosine_threshold_distance_mask(vec1, vec2, mask):
    '''
    Boolean mask over which features to compute cosine distance.
    Much faster than cosine_threshold_distance which would recompute the mask multiple times.
    '''
    distance = cosine(vec1[mask], vec2[mask])
    return distance if not np.isnan(distance) else 1.0

def cosine_area(vec1, vec2, weights):
    '''
    Compute the normalized area under the curve where y-axis is cosine distance
    and x-axis is the ratio threshold. Between 0.0 and 1.0, nans converted to 1.0
    '''
    max_weight = np.max(weights)
    delta = (max_weight - 1.0) / 100.0
    area = 0.0
    for threshold in np.arange(1.0, max_weight+delta, delta):
        delta_area = cosine(vec1[weights >= threshold], vec2[weights >= threshold])
        if np.isnan(delta_area):
            delta_area = 1.0
        area += delta_area
    return area / max_weight

def evaluate_ranking(ranking, positive_instances):
    '''
    Return average precision and precision at top k={10, 100, 200, 500, 1000}
    '''
    avg_p = average_precision(ranking, positive_instances)
    pr_10 = precision_recall_at_top_k(ranking, positive_instances, 10)
    pr_100 = precision_recall_at_top_k(ranking, positive_instances, 100)
    pr_200 = precision_recall_at_top_k(ranking, positive_instances, 200)
    pr_500 = precision_recall_at_top_k(ranking, positive_instances, 500)
    pr_1000 = precision_recall_at_top_k(ranking, positive_instances, 1000)
    pr_2000 = precision_recall_at_top_k(ranking, positive_instances, 2000)
    pr_5000 = precision_recall_at_top_k(ranking, positive_instances, 5000)
    pr_10000 = precision_recall_at_top_k(ranking, positive_instances, 10000)
    sys.stdout.write('Average precision: {}\n'.format(avg_p))
    sys.stdout.write('Precision at top k\n')
    sys.stdout.write('k\tprecision\n')
    sys.stdout.write('10\t{}\n'.format(pr_10))
    sys.stdout.write('100\t{}\n'.format(pr_100))
    sys.stdout.write('200\t{}\n'.format(pr_200))
    sys.stdout.write('500\t{}\n'.format(pr_500))
    sys.stdout.write('1000\t{}\n'.format(pr_1000))
    sys.stdout.write('2000\t{}\n'.format(pr_2000))
    sys.stdout.write('5000\t{}\n'.format(pr_5000))
    sys.stdout.write('10000\t{}\n'.format(pr_10000))

def average_precision(ranking, positive_instances):
    '''
    Given an ordered ranking of instances and a set of positive instances, 
    compute average precision.
    '''
    if len(positive_instances) == 0:
        return 1.0
    precision_sum = 0.0
    cur_num_positive = 0.0
    for idx, instance in enumerate(ranking):
        if instance in positive_instances:
            cur_num_positive += 1
            precision_sum += cur_num_positive / (idx + 1)
    return precision_sum / len(positive_instances)

def precision_recall_at_top_k(ranking, positive_instances, k):
    if k == 0:
        return 1.0, 0.0
    num_correct = 0
    for idx, instance in enumerate(ranking):
        if instance in positive_test_ids:
            num_correct += 1
        if idx == k-1:
            break
    return 1.0*num_correct/k, 1.0*num_correct/len(positive_instances)

def compute_feature_reference_distributions(test_sample_size, num_samples, base_matrix):
    total_rows = base_matrix.shape[0]
    idxs = np.arange(total_rows)
    denominator_mean_vector = np.array(csr_matrix.mean(base_matrix, axis=0)).ravel()
    ratios = []
    for i in range(num_samples):
        print(i)
        np.random.shuffle(idxs)
#         denominator_mean_vector = np.array(csr_matrix.mean(base_matrix[idxs[:total_rows-test_sample_size]], axis=0)).ravel()
        numerator_mean_vector = np.array(csr_matrix.mean(base_matrix[idxs[:test_sample_size]], axis=0)).ravel()
        cur_ratio = numerator_mean_vector/denominator_mean_vector
        print(cur_ratio)
        ratios.append(cur_ratio)
    return np.max(ratios, axis=0) 

if __name__ == '__main__':
    #     label = sys.argv[1]
    label = 'vegan'
    average_dir = '/data/user_documents/individual_posts_100000'
    label_dir = '/data/user_documents/individual_posts_{}'.format(label)
    
    # Get positive train/test user ids
    sys.stdout.write('Getting positive training and test ids and posts\n')    
    positive_train_ids_filename = label_dir + '/' + 'train-user-ids.txt'
    positive_train_ids = get_ids_from_file(positive_train_ids_filename)    
    positive_test_ids_filename = label_dir + '/' + 'test-user-ids.txt'
    positive_test_ids = get_ids_from_file(positive_test_ids_filename)
    
    # Get positive train/test user posts
    positive_post_filename = label_dir + '/' + 'all-individual-posts.txt'
    positive_train_user_posts, positive_test_user_posts = get_user_posts(positive_post_filename,
                                                                         set(positive_train_ids),
                                                                         set(positive_test_ids))
    positive_train_user_posts = order_user_posts(positive_train_user_posts, positive_train_ids)
    positive_test_user_posts = order_user_posts(positive_test_user_posts, positive_test_ids)
    
    # Get negative train/test user ids
    sys.stdout.write('Getting negative training and test ids and posts\n')
    negative_train_ids_filename = average_dir + '/' + 'train-user-ids.txt'    
    negative_train_ids = get_ids_from_file(negative_train_ids_filename)
    negative_test_ids_filename = average_dir + '/' + 'test-user-ids.txt'
    negative_test_ids = get_ids_from_file(negative_test_ids_filename)
    
    # Get negative train/test user posts
    negative_post_filename = average_dir + '/' + 'all-individual-posts.txt'
    negative_train_user_posts, negative_test_user_posts = get_user_posts(negative_post_filename,
                                                                         set(negative_train_ids),
                                                                         set(negative_test_ids))
    negative_train_user_posts = order_user_posts(negative_train_user_posts, negative_train_ids)
    negative_test_user_posts = order_user_posts(negative_test_user_posts, negative_test_ids)
    
    # Fit TfidfVectorizer over average set
    # Compute canonical average word vector
    # Cache TfidfVectorizer
    sys.stdout.write('Getting Tfidf Vectorizer trained on negative posts\n')
    canonical_word_vector_filename = average_dir + '/' + 'canonical_word_vector.out'
    vocabulary_filename = average_dir + '/' + 'tfidf_vocabulary.out'
    idf_filename = average_dir + '/' + 'idf_vector.out'
    tfidf_vectorizer = get_vectorizer(vocabulary_filename, idf_filename, 
                                      negative_train_user_posts, canonical_word_vector_filename)
    sys.stdout.write('Getting canonical vector for average users\n')
    canonical_vector_average = get_canonical_vector(negative_train_user_posts, tfidf_vectorizer, 
                                                    canonical_word_vector_filename)
    
    base_matrix = tfidf_vectorizer.transform([user_posts.posts for user_posts in negative_train_user_posts])
    max_ratios = compute_feature_reference_distributions(5006, 100, base_matrix)
    print(max_ratios)
    sys.exit()
    
    # Compute and cache word vectors for negative test set
    sys.stdout.write('Getting vectors for negative test posts\n')
    negative_test_vector_matrix_filename = average_dir + '/' + 'test_vector_matrix.mtx'
    negative_test_vector_mx = get_post_vectors(tfidf_vectorizer, negative_test_user_posts, 
                                               negative_test_vector_matrix_filename)
    
    # Compute and cache canonical label word vector
    sys.stdout.write('Getting canonical vector for {} users\n'.format(label))
    canonical_label_word_vector_filename = label_dir + '/' + 'canonical_word_vector.out'
    canonical_vector_label = get_canonical_vector(positive_train_user_posts, tfidf_vectorizer, 
                                                  canonical_label_word_vector_filename)
    # get_largest_word_differences(canonical_vector_label, canonical_vector_average, tfidf_vectorizer, k=50)
    
    # Compute and cache word vectors for positive test set
    sys.stdout.write('Getting vectors for positive test posts\n')
    positive_test_vector_matrix_filename = label_dir + '/' + 'test_vector_matrix.mtx'
    positive_test_vector_mx = get_post_vectors(tfidf_vectorizer, positive_test_user_posts, 
                                               positive_test_vector_matrix_filename)
    
    # Calculate distance between each word vector in positive and negative test set 
    # from canonical label word vector. Vary the distance function.
    sys.stdout.write('Sorting users by distances...\n')
    pos_neg_test_matrix = vstack([positive_test_vector_mx, negative_test_vector_mx])
#     sorted_user_distances_cosine = get_users_sorted_by_canonical_distance(
#                                                         positive_test_ids+negative_test_ids, 
#                                                         pos_neg_test_matrix,
#                                                         canonical_vector_label)
#     sys.stdout.write('Cosine\n')
#     evaluate_ranking([x[0] for x in sorted_user_distances_cosine], set(positive_test_ids))
    
    weights = canonical_vector_label/canonical_vector_average
    for threshold in [95]:#[50, 90, 95, 99]:
        mask = weights >= np.percentile(weights, threshold)
        sorted_user_distances_cosine_threshold = get_users_sorted_by_canonical_distance(
                                                        positive_test_ids+negative_test_ids, 
                                                        pos_neg_test_matrix, 
                                                        canonical_vector_label, 
                                                        distance=cosine_threshold_distance_mask, 
                                                        distance_kwargs={'mask': mask})
        sys.stdout.write('\nCosine {}%\n'.format(threshold))
        evaluate_ranking([x[0] for x in sorted_user_distances_cosine_threshold], set(positive_test_ids))