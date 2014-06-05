from db_utils import *
import os
import nltk
import random

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

def get_all_words(dir):
    '''
    For each file in dir, read the raw text and tokenize to get list of all words
    in the corpus.
    '''
    words = []
    for document in os.listdir(dir):
        file = open(dir + '/' + document, 'r')
        raw = file.read()
        words.extend(nltk.word_tokenize(raw))
    return words

def get_top_k_words(all_words, k):
    all_words = nltk.FreqDist(w.lower() for w in all_words)
    top_k_words = all_words.keys()[:k]
    return top_k_words

def document_features(document, word_features):
    document_words = set(document)
    features = {}
    for word in word_features:
        features['contains(%s)' % word] = (word in document_words)
        features["count(%s)>3" % word] = (document.count(word) > 3)
        features["count(%s)>7" % word] = (document.count(word) > 7)
        features["count(%s)>15" % word] = (document.count(word) > 15)
    return features

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

## Document manipulation with nltk ##
#document_dirs = ['25-34', 'less-18', '35-44', '65-greater', '45-54', '18-24', '55-64']
document_dirs = ['25-34', '55-64']
#corpus_words = []
#for document_dir in document_dirs:
#    corpus_words.extend( get_all_words(data_dir + '/' + document_dir) )
#print(len(corpus_words))
#top_words = get_top_k_words(corpus_words, 10000)
#print(top_words)

top_words = []
for document_dir in document_dirs:
    corpus_words = get_all_words(data_dir + '/' + document_dir)
    top_words.extend( get_top_k_words(corpus_words, 500) )

# get pairs of document words and document category
document_category_pairs = []
for document_dir in document_dirs:
    full_dir = data_dir + '/' + document_dir
    for document in os.listdir(full_dir):
        file = open(full_dir + '/' + document, 'r')
        raw = file.read()
        document_category_pairs.append( (nltk.word_tokenize(raw), document_dir) )
random.shuffle(document_category_pairs)

# create a list of document features and document category
print('getting features for each document...')
featuresets = [(document_features(d, top_words), c) for (d,c) in document_category_pairs]
# train, test folds
train_set, test_set = featuresets[100:], featuresets[:100]
# train naive bayes classifier
print('training naive bayes classifier...')
classifier = nltk.NaiveBayesClassifier.train(train_set)

print(nltk.classify.accuracy(classifier, test_set))
classifier.show_most_informative_features(100)

# TODO:
# - Work on tokenizer (stemming?, removing stop words, removing punctuation..except for #,!.)
# - Feature selection
# - Different models: SVM, decision trees
# - Adding documents (messages from photos and videos, crawling links, stripping html)
# - Adding liked documents (same set of sources)