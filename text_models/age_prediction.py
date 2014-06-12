from db_utils import *
import os
import nltk
import random
from happiestfuntokenizing import Tokenizer

def birth_year_to_age_range(birth_year):
    '''
    Convert a given birth year into a discrete category among 
    <18, 18-24, 25-34, 35-44, 45-54, 55-64, 65+ (the Nielsen age groups)
    '''
    current_year = 2014
    age = current_year - birth_year
    if age < 18:
        return 'less-18'
    elif 18 <= age <= 24:
        return '18-24'
    elif 25 <= age <= 34:
        return '25-34'
    elif 35 <= age <= 44:		
        return '35-44'
    elif 45 <= age <= 54:		
        return '45-54'
    elif 55 <= age <= 64:		
        return '55-64'
    else:
        return '65-greater'

def get_user_sample_with_birth_year(sample_size, data_dir, output_file_name, connnection):
    '''	
    Get sample_size random fb users, either sources (secondary) or targets (primary), 
    that have an observed value for birthday.
    Stores a table in redshift and saves a local file (that adds a column for age group)
    '''
    query = """
            CREATE TABLE fbid_sample_{sample_size} AS (
                SELECT fbid,
                       extract(year from min(birthday)) birth_year
                FROM users
                WHERE birthday is not null
                GROUP BY fbid
                ORDER BY random()
                LIMIT {sample_size}
            )
            """.format(sample_size=sample_size)
    execute_query(query, conn, fetchable=False)
    query = """
            SELECT *
            FROM fbid_sample_{sample_size}
            """.format(sample_size=sample_size)
    rows = execute_query(query, conn)
    output_file = open(data_dir + '/' + output_file_name, 'w')
    for row in rows:
        fbid, birth_year = row
        age_group = birth_year_to_age_range(birth_year)
        output_file.write('{}\t{}\t{}\n'.format(fbid, birth_year, age_group))
    output_file.close()


def get_user_posts_by_type(sample_size, post_type, conn):
    '''
    Create a subset of the posts table that includes all posts of type post_type posted by 
    the sampled fbid users.
    '''
    query = """
            CREATE TABLE fbid_sample_{sample_size}_posts_{post_type} AS (
                SELECT fbid_user, message
                FROM posts
                    JOIN fbid_sample_{sample_size} 
                        ON posts.fbid_user=fbid_sample_{sample_size}.fbid
                WHERE type = '{post_type}'
            )
            """.format(sample_size=sample_size, post_type=post_type)
    execute_query(query, conn, fetchable=False)
    print('DONE!')
    
def create_user_post_type_documents(data_dir, user_sample_file_name, sample_size, post_type, conn):
    '''
    For each sampled user, combine all messages of a given post type as a single document
    and save as a local file.
    '''
    file = open(data_dir + '/' + user_sample_file_name, 'r')
    fbid_to_age_range = {} # fbid -> age range
    for line in file:
        vals = line.strip().split('\t')
        fbid = vals[0]
        age_range = vals[2]
        fbid_to_age_range[fbid] = age_range
    file.close()
 
    for fbid in fbid_to_age_range:
        query = """
                SELECT message
                FROM fbid_sample_{sample_size}_posts_{post_type}
                WHERE fbid_user = {fbid}
                """.format(fbid=fbid, sample_size=sample_size, post_type=post_type)
        document = ""
        rows = execute_query(query, conn)
        if rows:
            for row in rows:
                if row[0]:
                    document += row[0] + '\n'
        
        age_range=fbid_to_age_range[fbid]
        sub_dir = data_dir + '/' + '{}'.format(age_range)
        if not os.path.exists(sub_dir):
            os.makedirs(sub_dir)
        user_document_file_name = sub_dir + '/' + '{fbid}_{post_type}.txt'.format(
                                    fbid=fbid, post_type=post_type)
        file = open(user_document_file_name, 'w')
        file.write(document)
        file.close()

def get_document_words(document_dirs):
    '''
    Given a list of directories, for each file, read the raw text and tokenize to get
    a list of all words in that document.
    Returns a dictionary from file to a list of words.
    '''
    document_to_words = {}
    for document_dir in document_dirs:
        for document in os.listdir(data_dir + '/' + document_dir):
            filename = data_dir + '/' + document_dir + '/' + document
            file = open(filename, 'r')
            raw = file.read()
            if raw: # prunes out empty documents (don't train/predict with them)
#                 document_to_words[(document_dir, document)] = [w.lower() for w in nltk.word_tokenize(raw)]
                document_to_words[(document_dir, document)] = [w.lower() for w in tok.tokenize(raw)]
            file.close()
    return document_to_words
    
def get_corpus_word_frequency(document_to_words):
    '''
    Given document_name -> word_list, collate and create a corpus-wide frequency distribution.
    Return frequency of all words that appear.
    '''
    corpus_words = []
    for words in document_to_words.values():
        corpus_words.extend(words)
    return nltk.FreqDist(corpus_words)
    
def get_top_k_words(corpus_frequencies, k):
    return corpus_frequencies.keys()[:k]

def document_count_threshold_features(document, word_features, thresholds):
    document_words = set(document)
    features = {}
    for word in word_features:
        for threshold in thresholds:
            features['count({})>{}'.format(word, threshold)] = document.count(word) > threshold
    return features
    
def document_contains_feature(document, word_features):
    document_words = set(document)
    features = {}
    for word in word_features:
        features['contains({})'.format(word)] = word in document_words
    return features

def naive_bayes_experiment(classes, top_ks, feature_methods_and_args, trials):
    document_dirs = classes
    print('reading all documents...')
    document_to_words = get_document_words(document_dirs)
    corpus_frequencies = get_corpus_word_frequency(document_to_words) 

    for top_k in sorted(top_ks):
        print('getting top {} words...'.format(top_k))
        top_words = get_top_k_words(corpus_frequencies, top_k)
        print(top_words)
        
        # create a list of document features and document category
        print('getting features for each document...')
        featuresets = []
        for (d,c) in [(words, doc_handle[0]) for doc_handle, words in document_to_words.items()]:
            document_features = {}
            for feature_method, args in feature_methods_and_args:
                document_features.update( feature_method(d, top_words, *args) )
            featuresets.append( (document_features, c) )

        for trial in range(trials):
            random.shuffle(featuresets)
        
            # train, test folds
            train_set = featuresets[int(len(featuresets)*0.1):]
            test_set = featuresets[:int(len(featuresets)*0.1)]

            # train naive bayes classifier
            print('training naive bayes classifier...')
            classifier = nltk.NaiveBayesClassifier.train(train_set)

            # classifier.show_most_informative_features(100)
            print(nltk.classify.accuracy(classifier, test_set))


## Parameters ##
sample_size = 1000
data_dir = 'data'
user_sample_file_name = 'user_sample_{}_with_birth_year.tsv'.format(sample_size)
post_type = 'status'

## Database operations ##
#conn = redshift_connect()
#get_user_sample_with_birth_year(sample_size, data_dir, user_sample_file_name, conn)
#get_user_posts_by_type(sample_size, post_type, conn)
#create_user_post_type_documents(data_dir, user_sample_file_name, sample_size, post_type, conn)
#redshift_disconnect(conn)

## Naive Bayes classifier experiment with nltk ##
tok = Tokenizer()
classes = ['less-18', '18-24', '25-34', '35-44', '45-54', '55-64', '65-greater']
top_ks = [100, 1000, 2000]
feature_methods_and_args = [(document_contains_feature, []), 
                            (document_count_threshold_features, [[3, 7, 15]])]
trials = 3
naive_bayes_experiment(classes, top_ks, feature_methods_and_args, trials)

