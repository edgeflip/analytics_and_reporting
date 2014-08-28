from db_utils import redshift_connect, redshift_disconnect, execute_query
from create_document_topic_matrix import get_document_to_topic_proportions_dict_with_label_list
from scipy.spatial.distance import cosine, wminkowski
import numpy as np
import operator
import joblib
import re

# Exploration:
# (1) - Compute average topic vector of 'vegan posts'
#     - For each user, compute average distance of posts to canonical vegan post
# (2) - Compute average topic vector of 'vegan users'
#     - For each user, compute distance to canonical vegan user
# Distances can be computed using any (e.g., cosine similarity) that shows up in scipy.spatial.distance
# Compare rankings of (1) and (2)

def get_name_from_fbid(conn, fbid):
    query = """
        SELECT fname, lname
        FROM users
        WHERE fbid = {}
        """.format(fbid)
    rows = execute_query(query, conn)    
    return '{fname} {lname}'.format(fname=rows[0][0], lname=rows[0][1])

def get_topic_to_top_words():
    topic_word_filename = '/data/topics/individual_posts_100000/individual-posts-2000-keys.txt'
    topic_to_top_words = {}
    file = open(topic_word_filename, 'r')
    for line in file:
        vals = re.split('\s+', line.strip())
        topic = int(vals[0])
        words = vals[2:]
        topic_to_top_words[topic] = words
    file.close()
    return topic_to_top_words

def get_post_to_topic_probabilities(fbid):
    data_dir = '/data/topics/individual_posts_test_users'
    document_label_filename = data_dir + '/' + 'individual-post-fbid-{}-ids.txt'.format(fbid)
    file = open(document_label_filename, 'r')
    labels = [line.strip() for line in file]
    file.close()
    document_topic_composition_filename = data_dir + '/' + 'individual-posts-fbid-{}-2000-composition.txt'.format(fbid)
    post_to_topic_probability = get_document_to_topic_proportions_dict_with_label_list(document_topic_composition_filename, 
                                                                                           labels)
    return post_to_topic_probability

def cache_post_to_topic_probabilities():
    data_dir = '/data/topics/individual_posts_vegans'
    document_label_filename = data_dir + '/' + 'individual-post-ids.txt'
    file = open(document_label_filename, 'r')
    labels = [line.strip() for line in file]
    file.close()
    document_topic_composition_filename = data_dir + '/' + 'individual-posts-2000-composition.txt'
    document_topic_dict_filename = data_dir + '/' + 'individual-posts-2000-topic-probabilities.out'
    post_to_topic_probability = get_document_to_topic_proportions_dict_with_label_list(document_topic_composition_filename, 
                                                                                           labels)
    joblib.dump(post_to_topic_probability, document_topic_dict_filename)

def create_topic_vector(topic_to_probabilities, num_topics):
    topic_vector = np.zeros(num_topics)
    for topic, prob in topic_to_probabilities.items():
        topic_vector[topic] = prob
    return topic_vector

def get_canonical_post_vector(post_to_topic_probability, num_topics):
    canonical_vector = np.zeros(num_topics)
    num_posts = len(post_to_topic_probability)
    for post in post_to_topic_probability:
        for topic, prob in post_to_topic_probability[post].items():
            canonical_vector[topic] += prob
    canonical_vector /= num_posts
    return canonical_vector
    
def get_largest_topic_differences(vector1, vector2, topic_to_top_words, k=10):
    ratio_dict = {i: ratio for i, ratio in enumerate(vector1/vector2)}
    sorted_ratios = sorted(ratio_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
    print('Top {}:'.format(k))
    for topic, ratio in sorted_ratios[:k]:
        print(topic, ratio, topic_to_top_words[topic][:10])
    print('\nBottom {}:'.format(k))
    bottom_k = sorted_ratios[-k:]
    bottom_k.reverse()
    for topic, ratio in bottom_k:
        print(topic, ratio, topic_to_top_words[topic][:10])

def get_user_topic_vector(user_to_posts, post_to_topic_probability):
    user_to_topic_probabilities = {}
    for user, posts in user_to_posts.items():
        user_to_topic_probabilities[user] = get_canonical_post_vector({post: post_to_topic_probability[post] for post in posts}, 2000)
    return user_to_topic_probabilities
 
def sort_users_by_canonical_distance(conn, user_to_topic_vectors, canonical_vector, k=20, distance=cosine, distance_kwargs={}):
    user_to_distance = {user: distance(topic_vector, canonical_vector, **distance_kwargs) for user, topic_vector in user_to_topic_vectors.items()}
    sorted_distances = sorted(user_to_distance.iteritems(), key=operator.itemgetter(1), reverse=False)
    print('Top {}:'.format(k))
    for user, distance in sorted_distances[:k]:
        print(user, get_name_from_fbid(conn, user), distance)
    print('\nBottom {}:'.format(k))
    bottom_k = sorted_distances[-k:]
    bottom_k.reverse()
    for user, distance in bottom_k:
        print(user, get_name_from_fbid(conn, user), distance)
    
if __name__ == '__main__':
    post_to_topic_probability_vegans = joblib.load('/data/topics/individual_posts_vegans/individual-posts-2000-topic-probabilities.out')
    canonical_vegan_post = get_canonical_post_vector(post_to_topic_probability_vegans, 2000)

    post_to_topic_probability_all_users = joblib.load('/data/topics/individual_posts_100000/individual-posts-topic-probabilities.out')
    canonical_post = get_canonical_post_vector(post_to_topic_probability_all_users, 2000)

    topic_to_top_words = get_topic_to_top_words()
    get_largest_topic_differences(canonical_vegan_post, canonical_post, topic_to_top_words, k=20)
    
    # how far is the average 'known' vegan from the canonical vegan post
    # for each user, compute average of each post to canonical topic vector
    
    vegan_users_to_posts = {}
    for post in post_to_topic_probability_vegans:
        user = post.split('_')[0]
        vegan_users_to_posts.setdefault(user, []).append(post)

    all_user_to_posts = {}
    for post in post_to_topic_probability_all_users:
        user = post.split('_')[0]
        all_user_to_posts.setdefault(user, []).append(post)
        
    vegan_user_to_topic_vector = get_user_topic_vector(vegan_users_to_posts, post_to_topic_probability_vegans)
    sort_users_by_canonical_distance(vegan_user_to_topic_vector, canonical_vegan_post)

    all_user_to_topic_vector = get_user_topic_vector(all_user_to_posts, post_to_topic_probability_all_users)
    sort_users_by_canonical_distance(all_user_to_topic_vector, canonical_vegan_post)
    
    #sort_users_by_canonical_distance(all_user_to_topic_vector, canonical_vegan_post, distance=wminkowski, distance_kwargs={'p': 2, 'w': canonical_vegan_post/canonical_post})
    
    # Testing "posts' average distance"
#     user_to_distances = {}
#     for user, posts in all_user_to_posts.items():
#         user_to_distances[user] = []
#         for post in posts:
#             user_to_distances[user].append(cosine(create_topic_vector(post_to_topic_probability_all_users[post], 2000), canonical_vegan_post))
# 
#     for user in user_to_distances:
#         user_to_distances[user] = 1.0*sum(user_to_distances[user])/len(user_to_distances[user])
#     
#     k=20
#     sorted_distances = sorted(user_to_distances.iteritems(), key=operator.itemgetter(1), reverse=False)
#     print('Top {}:'.format(k))
#     for user, distance in sorted_distances[:k]:
#         print(user, distance)
#     
#     print('\nBottom {}:'.format(k))
#     bottom_k = sorted_distances[-k:]
#     bottom_k.reverse()
#     for user, distance in bottom_k:
#         print(user, distance) 


jesse_friend_to_posts = {}
for post in jesse_friend_post_topic_probabilities:
    user = post.split('_')[0]
    jesse_friend_to_posts.setdefault(user, []).append(post)

jesse_friend_to_topic_vector = get_user_topic_vector(jesse_friend_to_posts, jesse_friend_post_topic_probabilities)
sort_users_by_canonical_distance(conn, jesse_friend_to_topic_vector, canonical_vegan_post)


conn = redshift_connect()

redshift_disconnect(conn)