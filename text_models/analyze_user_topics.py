from sklearn_prediction_utils import models_report
from create_document_topic_matrix import get_document_to_topic_proportions_dict_with_label_list, get_document_to_topic_proportions_dict
from create_document_topic_matrix import document_to_feature_matrix
from sklearn.linear_model import Ridge
from sklearn.svm import LinearSVC
from sklearn.grid_search import GridSearchCV
from scipy.sparse import csc_matrix
from scipy.io import mmread
from collections import Counter
import numpy as np
import operator
import random
import os
import re
import sys
import joblib

def get_document_words(filename):
    f = open(filename, 'r')
    document_words = re.findall('(?:[a-zA-Z]{2,})', f.read())
    f.close()
    return [word.lower() for word in document_words if word.lower() not in stopwords]

def get_topic_probability(document_words):
    word_counts = Counter(document_words)
    doc_unique_words = set(word_counts.keys())
    total_word_count = len(document_words)
    topic_to_probability = {}
    if total_word_count == 0:
        return {topic: 0.0 for topic in topic_to_word_weights}
    for topic, word_weights in topic_to_word_weights.items():
        topic_prob = 0.0
        common_words = doc_unique_words & set(word_weights.keys())
        for word in common_words:
            topic_prob += word_weights[word]*word_counts[word]/total_word_count
        topic_to_probability[topic] = topic_prob
    total_weight = sum(topic_to_probability.values())
    if total_weight > 0:
        topic_to_probability = {topic: probability/total_weight for topic, probability in topic_to_probability.items()}
    return topic_to_probability

def sort_users_by_topic_probability(user_to_topic_probabilities, topics, k=20):
    user_to_probability = {}
    for user in user_to_topic_probabilities:
        user_to_probability[user] = 0.0
        for topic in topics:
            if topic in user_to_topic_probabilities[user]:
                user_to_probability[user] += user_to_topic_probabilities[user][topic]
    user_prob_sorted = sorted(user_to_probability.iteritems(), key=operator.itemgetter(1), reverse=True)
    print('Top {}:'.format(k))
    for user, prob in user_prob_sorted[:k]:
        print(user, prob)
    print('\nBottom {}:'.format(k))
    bottom_k = user_prob_sorted[-k:]
    bottom_k.reverse()
    for user, prob in bottom_k:
        print(user, prob)

def sort_users_by_topic_probability_normalized(user_to_topic_probabilities, positive_topics, negative_topics, k=20):
    user_to_probability = {}
    for user in user_to_topic_probabilities:
        user_total_weight = sum(user_to_topic_probabilities[user].values())
        if user_total_weight < 2:
            continue
        user_to_probability[user] = 0.0
        for topic in positive_topics:
            if topic in user_to_topic_probabilities[user]:
                user_to_probability[user] += user_to_topic_probabilities[user][topic]
        for topic in negative_topics:
            if topic in user_to_topic_probabilities[user]:
                user_to_probability[user] -= user_to_topic_probabilities[user][topic]
        if user_total_weight > 0.0:
            user_to_probability[user] /= user_total_weight
    user_prob_sorted = sorted(user_to_probability.iteritems(), key=operator.itemgetter(1), reverse=True)
    print('Top {}:'.format(k))
    for user, prob in user_prob_sorted[:k]:
        print(user, prob)
    print('\nBottom {}:'.format(k))
    bottom_k = user_prob_sorted[-k:]
    bottom_k.reverse()
    for user, prob in bottom_k:
        print(user, prob)

def user_to_topic_probs_from_post_to_topic_probs(user_to_posts, post_to_topic_probability):
    user_to_topic_probabilities = {}
    for user, posts in user_to_posts.items():
        for post in posts:
            for topic, prob in post_to_topic_probability[post].items():
                user_to_topic_probabilities.setdefault(user, {})
                user_to_topic_probabilities[user].setdefault(topic, 0.0)
                user_to_topic_probabilities[user][topic] += prob
    return user_to_topic_probabilities


if __name__ == '__main__':
#     stopwords_file = 'en.txt'
#     f = open(stopwords_file, 'r')
#     stopwords = set([line.strip() for line in f])
#     f.close()
# 
#     data_dir = '/data/topics/individual_posts'
#     topic_word_normalized_weights_filename = 'individual-posts-default-regex-1000-topic-word-normalized-weights.txt'
#     user_document_dir = '/data/user_documents/all_originating_posts'
#     in_file = open(data_dir + '/' + topic_word_normalized_weights_filename, 'r')
# 
#     word_to_topic_weights = {}
#     topic_to_word_weights = {}
#     for line in in_file:
#         topic, word, weight = line.strip().split('\t')
#         topic = int(topic)
#         weight = float(weight)
#     
#         word_to_topic_weights.setdefault(word, {})
#         word_to_topic_weights[word][topic] = weight
#     
#         topic_to_word_weights.setdefault(topic, {})
#         topic_to_word_weights[topic][word] = weight


#     users_cache = '/data/caches/user_and_from_friend_posts/users_5000_cache.out'
#     users_age_cache = '/data/caches/user_and_from_friend_posts/outcome_5000_age_cache.out'
#     users_gender_cache = '/data/caches/user_and_from_friend_posts/outcome_5000_gender_cache.out'
#     filenames_cache = '/data/caches/user_and_from_friend_posts/filenames_5000_user_posts_cache.out'
#     user_features_cache = '/data/caches/user_post_topic_proportions_5000.mtx'
#     users = joblib.load(users_cache)
#     ages = joblib.load(users_age_cache)
#     genders = joblib.load(users_gender_cache)
#     filenames = joblib.load(filenames_cache)
    
    sys.stdout.write('Getting post to topic probabilities\n')
    individual_post_topic_filename = '/data/topics/individual_posts_100000/individual-posts-2000-composition.txt'
    post_to_topic_probability = get_document_to_topic_proportions_dict([individual_post_topic_filename], strip_document_path=False)
    joblib.dump(post_to_topic_probability, '/data/topics/individual_posts_100000/individual-posts-topic-probabilities.out')
    post_to_topic_probability = joblib.load('/data/topics/individual_posts_100000/individual-posts-topic-probabilities.out')
    
    user_to_posts = {}
    for post in post_to_topic_probability:
        user = post.split('_')[0]
        user_to_posts.setdefault(user, []).append(post)
    
    sys.stdout.write('Building user to topic probabilities from posts\n')
    user_to_topic_probabilities = user_to_topic_probs_from_post_to_topic_probs(user_to_posts, post_to_topic_probability)
            
#     user_to_topic_probabilities = {}
#     ctr = 0
#     for user, filename in zip(users, filenames):
#         ctr += 1
#         document_words = get_document_words(filename)
#         topic_to_probability = get_topic_probability(document_words)
#         user_to_topic_probabilities[user] = topic_to_probability
#         if ctr % 10 == 0:
#             print(ctr)
            
#     user_to_topic_probabilities = {}
#     ctr = 0    
#     for user, topic_to_probability in zip(users, joblib.Parallel(n_jobs=-1)(joblib.delayed(get_topic_probability)(document_words) 
#                                         for document_words in joblib.Parallel(n_jobs=-1)(joblib.delayed(get_document_words)(filename) 
#                                                 for filename in filenames))):
#         ctr += 1
#         user_to_topic_probabilities[user] = topic_to_probability
#         if ctr % 10 == 0:
#             print(ctr)
# 
#     user_feature_matrix = document_to_feature_matrix(users, user_to_topic_probabilities, 1000, user_features_cache)
#     user_feature_matrix = csc_matrix(mmread(user_features_cache))

# # this block is from applying topic models to all user posts from individual post topics
#     sys.stdout.write('Getting list of all users in order\n')
#     all_users = []
#     all_users_file = open('/data/topics/user_posts_from_individual/user-posts.txt', 'r')
#     for line in all_users_file:
#         all_users.append(line.split()[0].split('_')[0])
#     all_users_file.close()
#     all_users = np.array(all_users)
#     sys.stdout.write('Getting user to topic probabilities\n')
#     user_to_topic_probabilities = get_document_to_topic_proportions_dict_with_label_list('/data/topics/user_posts_from_individual/user-posts-1000-composition.txt' ,all_users)
#     sys.stdout.write('Creating feature matrix\n')
#     user_feature_matrix = document_to_feature_matrix(users, user_to_topic_probabilities, 1000, user_features_cache)
#     user_feature_matrix = csc_matrix(mmread(user_features_cache))
# 
#     X = user_feature_matrix
#     y = ages
#     sample_size = X.shape[0]
#     score_type = 'continuous'
#     # models = [(Ridge, {'alpha': 0.001}, True, False)]
#     alphas = [x/100.0 for x in range(5, 201, 5)]
#     models = [(GridSearchCV, {'estimator': Ridge(copy_X=False), 
#                                'param_grid': [{'alpha': alphas}], 
#                                'cv': 5, 
#                                'scoring': 'r2',
#                                'verbose': 3}, True, True) ] 
# 
#     models_report(models, X, y, sample_size, score_type)
# 
#     y = genders
#     score_type = 'discrete'
# #     models = [(LinearSVC, {'C': 500}, True, False)]
#     cs = [1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5]
#     models = [(GridSearchCV, {'estimator': LinearSVC(), 
#                                'param_grid': [{'C': cs}], 
#                                'cv': 5, 
#                                'scoring': 'accuracy',
#                                'verbose': 3}, True, True) ]
#     models_report(models, X, y, sample_size, score_type)


    # user_to_topic_probabilities = {}
    # limit = 500
    # ctr = 0
    # all_documents = os.listdir(user_document_dir)
    # random.shuffle(all_documents)
    # for document in all_documents:
    #     if ctr == limit:
    #         break
    #     user_document_filename = user_document_dir + '/' + document
    #     user = document.split('_')[0]
    #     document_words = get_document_words(user_document_filename)
    #     if len(document_words) < 100:
    #         continue
    #     topic_to_probability = get_topic_probability(document_words)
    #     user_to_topic_probabilities[user] = topic_to_probability
    #     ctr += 1
    #     if ctr % 10 == 0:
    #         sys.stdout.write('.')
#         print(ctr)
#         sys.stdout.flush()
# #     highest_topic, highest_prob = max(topic_to_probability.iteritems(), key=operator.itemgetter(1))
# #     print(document, highest_topic, highest_prob)
# sys.stdout.write('\n')
# sys.stdout.flush()
# 
#       FIRST TOPIC RUN
#     topics = [34, 165, 309, 377, 522, 532, 806, 898, 908, 919] # african-american (n***a, africa, black, mlk)
#     topics = [810] # vegan --- needs help
#     topics = [802] # veteran --- needs help
    # # topics = [38, 142, 164, 293, 369, 40, 484, 505, 558, 790, 977] # hispanic - spanish words
    # # topics = [108, 365, 386, 452, 845] # star wars, ninja turtles, nintendo, gamers, nerd --- needs help
    # # topics = [2, 20, 76, 218, 226, 248, 425, 462, 555, 663, 958] # sporty/athletes (football, baseball, basketball, gym) --- needs help
#     topics = [383, 709] # smoking/drugs
    # topics = [2, 37, 187, 575] # trying to lose weight (weight, exercise, food)  --- needs help
    # topics = [300, 380, 395] # married (husband, wife, married, anniversary)
#     topics = [7, 140, 233, 295, 450, 521, 548, 837, 933] # parents (parent, baby, daughter, son)

    # SECOND TOPIC RUN (100000)
    positive_topics = [1449, 1553, 1718] # vegan, farmer's market, organic
    negative_topics = [426, 814, 1091, 1353, 1544, 1600, 1685] # general food, restaurant
    
    sys.stdout.write('Sorting users\n')
    sort_users_by_topic_probability(user_to_topic_probabilities, topics)
    sort_users_by_topic_probability_normalized(user_to_topic_probabilities, positive_topics, negative_topics)