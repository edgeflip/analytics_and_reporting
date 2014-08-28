from db_utils import redshift_connect, redshift_disconnect, execute_query
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.spatial.distance import cosine, wminkowski
from scipy.sparse import csr_matrix
from collections import namedtuple
from scipy.io import mmwrite
import numpy as np
import subprocess
import operator
import joblib
import sys
import re
import os

User_Posts = namedtuple('User_Posts', ['fbid', 'posts'])

def usage():
    sys.stderr.write("""
Usage: python rank_friends_by_word_distance.py fbid label
    fbid  - a facebook identifier
    label - a canonical type [vegan, smoker]
Analyzes a given facebook user's ('fbid') friends according to how
far their posts are from a canonical user of type 'label'

""")
    sys.exit()

def get_average_user_words(all_posts_filename, tfidf_vectorizer_filename, canonical_word_vector_filename):
    if os.path.isfile(tfidf_vectorizer_filename) and os.path.isfile(canonical_word_vector_filename):
        tfidf_vectorizer = joblib.load(tfidf_vectorizer_filename)
        canonical_word_vector = joblib.load(canonical_word_vector_filename).ravel()
    else:        
        user_posts_list = get_user_posts(all_posts_filename)
        tfidf_vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_df=0.5, min_df=60)
        mx = tfidf_vectorizer.fit_transform([user_posts.posts for user_posts in user_posts_list])
        joblib.dump(tfidf_vectorizer, tfidf_vectorizer_filename)
        canonical_word_vector = np.array(csr_matrix.mean(mx, axis=0)).ravel()
        joblib.dump(canonical_word_vector, canonical_word_vector_filename)
    return tfidf_vectorizer, canonical_word_vector

def create_individual_post_document_file(conn, fbid, document_dir):
    '''
    Given an fbid, get all non-empty messages in the posts table attributed to fbid's 
    friends. Creates a local file with each message on a new line.
    If the file already exists, it doesn't execute the query.
    '''
    post_document_filename = document_dir + '/' + 'all-individual-posts-fbid-{}.txt'.format(fbid)
    if os.path.isfile(post_document_filename):
        sys.stdout.write('\tFriend posts already exists for {}\n'.format(fbid))
    else:
        query = """
                SELECT fbid_post, message
                FROM (            
                    SELECT DISTINCT fbid_source as fbid
                    FROM edges
                    WHERE fbid_target = {fbid}
                ) friends
                    JOIN posts ON friends.fbid = posts.fbid_user
                WHERE posts.fbid_user = posts.post_from AND posts.message <> ''
            """.format(fbid=fbid)
        rows = execute_query(query, conn)
    
        file = open(post_document_filename, 'w')
        for row in rows:
            fbid_post, message = row
            message = ' '.join(re.split('\s+', message))
            file.write("{fbid_post} {message}\n".format(fbid_post=fbid_post, message=message))
        file.close()
        sys.stdout.write('Done getting posts for friends of {}\n'.format(fbid))
    return post_document_filename

def get_user_posts(post_document_filename):
    user_to_posts = {}
    post_document_file = open(post_document_filename, 'r')
    for line in post_document_file:
        vals = line.split()
        user = vals[0].split('_')[0]
        words = ' '.join(vals[1:])
        user_to_posts.setdefault(user, '')
        user_to_posts[user] += ' ' + words
    return [User_Posts(user, posts) for user, posts in user_to_posts.items()]

def get_canonical_vector(label, tfidf_vectorizer):
    label_topic_dir = '/data/user_documents/individual_posts_{label}'.format(label=label)    
    cached_canonical_vector_filename = label_topic_dir + '/' + 'canonical_{}_word_vector.out'.format(label)
    if os.path.isfile(cached_canonical_vector_filename):
        sys.stdout.write('\tReading in cached canonical word vector for {}\n'.format(label))
        return joblib.load(cached_canonical_vector_filename).ravel()
    else:
        posts_filename = label_topic_dir + '/' + 'all-individual-posts.txt'
        user_posts_list = get_user_posts(posts_filename)
        mx = tfidf_vectorizer.transform([user_posts.posts for user_posts in user_posts_list])
        canonical_word_vector = np.array(csr_matrix.mean(mx, axis=0)).ravel()
        joblib.dump(canonical_word_vector, cached_canonical_vector_filename)
        return canonical_word_vector

def get_users_sorted_by_canonical_distance(user_list, user_to_word_vector_mx, canonical_vector, distance=cosine, distance_kwargs={}):
    user_to_distance = {}
    for user_idx, user in enumerate(user_list):
        user_to_distance[user] = distance(user_to_word_vector_mx.getrow(user_idx).toarray().ravel(), canonical_vector, **distance_kwargs)
    sorted_user_distances = sorted(user_to_distance.iteritems(), key=operator.itemgetter(1), reverse=False)
    return sorted_user_distances

def get_top_and_bottom_k(conn, sorted_user_distances, k=20):
    print('Top {}:'.format(k))
    for user, distance in sorted_distances[:k]:
        print(user, get_name_from_fbid(conn, user), distance)
    print('\nBottom {}:'.format(k))
    bottom_k = sorted_distances[-k:]
    bottom_k.reverse()
    for user, distance in bottom_k:
        print(user, get_name_from_fbid(conn, user), distance)

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

def cosine_threshold_distance(vec1, vec2, weights):
    threshold = np.percentile(weights, 95)
    distance = cosine(vec1[weights >= threshold], vec2[weights >= threshold])
    if np.isnan(distance):
        distance = 1.0
    return distance

def cosine_ratio_distance(vec1, vec2, normalizer):
    return cosine(vec1/normalizer, vec2/normalizer)

def get_name_from_fbid(conn, fbid):
    return ""
#     query = """
#         SELECT fname, lname
#         FROM users
#         WHERE fbid = {}
#         """.format(fbid)
#     rows = execute_query(query, conn)    
#     return '{fname} {lname}'.format(fname=rows[0][0], lname=rows[0][1])

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

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
    
    fbid = sys.argv[1]
    label = sys.argv[2]
    if len(sys.argv) > 3:
        post_document_filename = sys.argv[3]
        fbid = None
    sys.stdout.write('Analyzing friend list for user {fbid} according to the \
distance from the canonical {label} user\n'.format(fbid=fbid, label=label))
    
    document_dir = '/data/user_documents/individual_posts_test_users'
    average_user_dir =  '/data/user_documents/individual_posts_100000'
    average_user_post_document_filename = average_user_dir + '/' + 'all-individual-posts.txt'
    tfidf_vectorizer_filename = average_user_dir + '/' + 'tfidf_vectorizer_100000.out'
    canonical_word_vector_filename = average_user_dir + '/' + 'canonical_100000_word_vector.out'
    conn = redshift_connect()

    if fbid:    
        # (1) Create a local file for the individual posts from all friends of fbid
        sys.stdout.write("Getting friends' posts...\n")        
        post_document_filename = create_individual_post_document_file(conn, fbid, document_dir)
        if os.stat(post_document_filename).st_size == 0:
            sys.stdout.write("\tFound no friends' posts for {}\n".format(fbid))
            sys.exit()

    # (1) Make sure average user tfidf vectorizer and canonical vector is cached
    sys.stdout.write('Getting tfidf vectorizer and canonical word vector for average users...\n')
    tfidf_vectorizer, canonical_vector_avg = get_average_user_words(average_user_post_document_filename, 
                                                                    tfidf_vectorizer_filename, 
                                                                    canonical_word_vector_filename)

    # (2) Load in canonical label word vector
    sys.stdout.write('Computing canonical word vector for {} users...\n'.format(label))
    canonical_vector_label = get_canonical_vector(label, tfidf_vectorizer)
#     get_largest_word_differences(canonical_vector_label, canonical_vector_avg, tfidf_vectorizer, k=50)

    # (3) Read in individual posts and create a sparse matrix of user X tfidf for the words
    sys.stdout.write("Creating tfidf matrix...\n")
    user_posts_list = get_user_posts(post_document_filename)
    user_word_vector_mx = tfidf_vectorizer.transform([user_posts.posts for user_posts in user_posts_list])
    
    # (4) Rank friends by distance to canonical topic vector
    user_list = [user_post.fbid for user_post in user_posts_list]
#     get_users_sorted_by_canonical_distance(user_list, user_to_word_vector_mx, canonical_vector_label)
    sorted_user_distances = get_users_sorted_by_canonical_distance(user_list, user_word_vector_mx, canonical_vector_label, distance=cosine_threshold_distance, distance_kwargs={'weights': canonical_vector_label/canonical_vector_avg})
    sorted_user_distances = get_users_sorted_by_canonical_distance(user_list, user_word_vector_mx, canonical_vector_label, distance=cosine_area, distance_kwargs={'weights': canonical_vector_label/canonical_vector_avg})
    get_top_and_bottom_k(conn, sorted_user_distances, k=20)
#     sort_users_by_canonical_distance(conn, [user_post.fbid for user_post in user_posts_list], user_word_vector_mx, canonical_vector_label)
#     sort_users_by_canonical_distance(conn, [user_post.fbid for user_post in user_posts_list], user_word_vector_mx, canonical_vector_label, distance=cityblock)
#     sort_users_by_canonical_distance(conn, [user_post.fbid for user_post in user_posts_list], user_word_vector_mx, canonical_vector_label, distance=cosine_threshold_distance, distance_kwargs={'weights': canonical_vector_label/canonical_vector_avg})
#     sort_users_by_canonical_distance(conn, [user_post.fbid for user_post in user_posts_list], user_word_vector_mx, canonical_vector_label, distance=cosine_area, distance_kwargs={'weights': canonical_vector_label/canonical_vector_avg})
#     sort_users_by_canonical_distance(conn, [user_post.fbid for user_post in user_posts_list], user_word_vector_mx, canonical_vector_label, distance=cosine_ratio_distance, distance_kwargs={'normalizer': canonical_vector_avg})
#     sort_users_by_canonical_distance(conn, [user_post.fbid for user_post in user_posts_list], user_word_vector_mx, canonical_vector_label, distance=weighted_distance, distance_kwargs={'weights': canonical_vector_label/canonical_vector_avg})
    redshift_disconnect(conn)