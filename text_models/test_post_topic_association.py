from sklearn_prediction_utils import models_report, get_predictions_across_folds
from db_utils import redshift_connect, redshift_disconnect, execute_query
from create_document_topic_matrix import get_document_to_topic_proportions_dict, document_to_feature_matrix
from scipy.stats import pearsonr, pointbiserialr, mode
from scipy.sparse import csc_matrix
from scipy.io import mmread
from sklearn.linear_model import Ridge
from sklearn.svm import LinearSVC
from sklearn.grid_search import GridSearchCV
from sklearn.preprocessing import LabelEncoder
import numpy as np
import joblib

def get_users_to_posts(conn):
    query = """
            SELECT fbid_user, fbid_post
            FROM fbid_sample_50000_messages
            WHERE message != ''
            """
    rows = execute_query(query, conn)      
    user_to_posts = {} # fbid_user -> [fbid_post, ...]
    for row in rows:
        fbid_user, fbid_post = str(row[0]), str(row[1])
        user_to_posts.setdefault(fbid_user, []).append(fbid_post)
    return user_to_posts


if __name__ == '__main__':
    cache_dir = '/data/caches/predict_individual_posts'
    cached = True
    user_to_posts_filename = cache_dir + '/' + 'user_posts_50000.out'
    user_to_birth_year_filename = cache_dir + '/' + 'user_birth_year_50000.out'
    user_to_gender_filename = cache_dir + '/' + 'user_gender_50000.out'
    post_to_topics_filename = cache_dir + '/' + 'post_to_topics_50000.out'
    post_to_birth_year_filename = cache_dir + '/' + 'post_birth_year_50000.out'
    post_to_gender_filename = cache_dir + '/' + 'post_gender_50000.out'
    post_to_feature_filename = cache_dir + '/' + 'post_features_50000.out.mtx'

    if not cached:
        # get user to links
        conn = redshift_connect()
        user_to_posts = get_users_to_posts(conn)
        redshift_disconnect(conn)
        joblib.dump(user_to_posts, user_to_posts_filename)
    
        # get user to labels
        user_sample_file = '/data/user_samples/user_sample_50000_with_birth_year_and_gender.tsv'
        user_to_birth_year = {}
        user_to_gender = {}
        for user_filename in [user_sample_file]:
            user_file = open(user_filename, 'r')
            for line in user_file:
                vals = line.strip().split('\t')
                user_to_birth_year[vals[0]] = int(vals[1])
                user_to_gender[vals[0]] = vals[3]
            user_file.close()
    
        joblib.dump(user_to_birth_year, user_to_birth_year_filename)
        joblib.dump(user_to_gender, user_to_gender_filename)

        # get post to topic proportions
        topic_proportions_filenames = ['/data/topics/individual_posts/individual-posts-default-regex-1000-composition.txt']
        post_to_topic_proportions = get_document_to_topic_proportions_dict(topic_proportions_filenames, 
                                                                           strip_document_path=False)
        joblib.dump(post_to_topic_proportions, post_to_topics_filename)

        # get post to labels
        post_to_birth_year = {}
        post_to_gender = {}
        for user, posts in user_to_posts.items():
            for post in posts:
                if post in post_to_topic_proportions:
                    post_to_birth_year[post] = user_to_birth_year[user]
                    post_to_gender[post] = user_to_gender[user]
        joblib.dump(post_to_birth_year, post_to_birth_year_filename)
        joblib.dump(post_to_gender, post_to_gender_filename)
        
    else:
        user_to_posts = joblib.load(user_to_posts_filename)
        user_to_birth_year = joblib.load(user_to_birth_year_filename)
        user_to_gender = joblib.load(user_to_gender_filename)
        post_to_topic_proportions = joblib.load(post_to_topics_filename)
        post_to_birth_year = joblib.load(post_to_birth_year_filename)
        post_to_gender = joblib.load(post_to_gender_filename)

        # get array of all links for alignment
        posts = np.array(post_to_birth_year.keys())
        post_to_row_number = {}
        for idx, post in enumerate(posts):
            post_to_row_number[post] = idx
        
        # create post to topic proportion matrix
#         post_feature_matrix = document_to_feature_matrix(posts, post_to_topic_proportions, 1000, post_to_feature_filename)
        post_feature_matrix = csc_matrix(mmread(post_to_feature_filename))
        birth_years = np.array([post_to_birth_year[post] for post in posts])
        genders = np.array([post_to_gender[post] for post in posts])
        
        print('done reading in.')
        
#         label_encoder = LabelEncoder()
#         genders = label_encoder.fit_transform(genders)
#         threshold = 0.1
#         num_rows, num_cols = post_feature_matrix.shape
#         for i in range(num_cols):
#             a = np.array(post_feature_matrix[:,i].todense()).reshape((num_rows,))
#             r, pval = pearsonr(a, birth_years)
#             if r > threshold or r < -threshold:
#                 print('Topic {} for age'.format(i))
#                 print(r, pval)
#                 print()
#             r, pval = pointbiserialr(a, genders)
#             if r > threshold or r < -threshold:
#                 print('Topic {} for gender'.format(i))
#                 print(r, pval)
#                 print()

    # For each link, get an age and gender prediction by training/testing in folds
    X = post_feature_matrix
    y = birth_years
    sample_size = X.shape[0]
    score_type = 'continuous'
#     models = [ (Ridge, {'alpha': 2.0}, True, False) ]
#     alphas = [x/100.0 for x in range(5, 201, 5)]
#     models = [(GridSearchCV, {'estimator': Ridge(copy_X=False), 
#                                'param_grid': [{'alpha': alphas}], 
#                                'cv': 5, 
#                                'scoring': 'r2',
#                                'verbose': 3}, True, True) ] 

#     models_report(models, X, y, sample_size, score_type)
    model = (Ridge, {'alpha': 2.0}, True, False)
    birth_year_preds = get_predictions_across_folds(X, y, model[0], model[1], model[2], sample_size)

    y = genders
    score_type = 'discrete'
#     models = [ (LinearSVC, {'C': 1.0}, True, False) ]
#     cs = [1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5]
#     models = [(GridSearchCV, {'estimator': LinearSVC(), 
#                                'param_grid': [{'C': cs}], 
#                                'cv': 5, 
#                                'scoring': 'accuracy',
#                                'verbose': 3}, True, True) ]
#     models_report(models, X, y, sample_size, score_type)
    model = (LinearSVC, {'C': 1.0}, True, False)
    gender_preds = get_predictions_across_folds(X, y, model[0], model[1], model[2], sample_size)
    
    # For each user, get distribution of predictions for each link the user has
    user_to_predicted_birth_years = {}
    user_to_predicted_genders = {}
    for user, posts in user_to_posts.items():
        for post in posts:
            if post in post_to_row_number:
                row_num = post_to_row_number[post]
                user_to_predicted_birth_years.setdefault(user, []).append(birth_year_preds[row_num])
                user_to_predicted_genders.setdefault(user, []).append(gender_preds[row_num])

    user_to_predicted_birth_year_mean = {}
    for user, predicted_birth_years in user_to_predicted_birth_years.items():
        predicted_birth_year_mean = sum(predicted_birth_years)/len(predicted_birth_years)
        user_to_predicted_birth_year_mean[user] = predicted_birth_year_mean

    user_to_predicted_gender_majority_vote = {}
    for user, predicted_genders in user_to_predicted_genders.items():
        predicted_gender_mode = mode(predicted_genders)[0][0]
        user_to_predicted_gender_majority_vote[user] = predicted_gender_mode
    
    # Get correlation of predicted birth year mean and true, accuracy for gender
    user_genders = []
    user_predicted_genders = []
    for user, predicted_gender in user_to_predicted_gender_majority_vote.items():
        user_predicted_genders.append(predicted_gender)
        user_genders.append(user_to_gender[user])
    user_genders = np.array(user_genders)
    user_predicted_genders = np.array(user_predicted_genders)
    
    user_birth_years = []
    user_predicted_birth_years = []
    for user, predicted_birth_year in user_to_predicted_birth_year_mean.items():
        user_predicted_birth_years.append(predicted_birth_year)
        user_birth_years.append(user_to_birth_year[user])
    user_birth_years = np.array(user_birth_years)
    user_predicted_birth_years = np.array(user_predicted_birth_years)

    gender_accuracy = np.mean(user_genders == user_predicted_genders)
    print(gender_accuracy)
    birth_year_cor = pearsonr(user_birth_years, user_predicted_birth_years)
    print(birth_year_cor)