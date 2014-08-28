from create_document_topic_matrix import get_document_to_topic_proportions_dict_with_label_list
from db_utils import redshift_connect, redshift_disconnect, execute_query
from scipy.spatial.distance import cosine, wminkowski
import numpy as np
import subprocess
import operator
import joblib
import sys
import re
import os

def usage():
    sys.stderr.write("""
Usage: python rank_friends_by_distance.py fbid label
    fbid  - a facebook identifier
    label - a canonical type [vegan, smoker]
Analyzes a given facebook user's ('fbid') friends according to how
far their posts are from a canonical user of type 'label'

""")
    sys.exit()

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

def import_mallet_file(input_filename, output_filename, piped_in_filename):
    if os.path.isfile(output_filename):
        sys.stdout.write('\tMallet input file already exists for {}\n'.format(fbid))
    else:
        mallet_import_command = """sudo /home/ubuntu/mallet-2.0.7/bin/mallet import-file \
--input {input_filename} \
--output {output_filename} \
--keep-sequence \
--remove-stopwords \
--use-pipe-from {piped_in_filename}""".format(
        input_filename=input_filename, 
        output_filename=output_filename,
        piped_in_filename=piped_in_filename)
        command_seq = mallet_import_command.split()
        process = subprocess.Popen(command_seq)
        process.communicate()

def infer_mallet_topics(input_filename, output_filename, mallet_inferencer):
    if os.path.isfile(output_filename):
        sys.stdout.write('\tTopics were already inferred for {}\n'.format(fbid))
    else:
        mallet_inference_command = """sudo /home/ubuntu/mallet-2.0.7/bin/mallet infer-topics \
--input {input_filename} \
--output-doc-topics {output_filename} \
--inferencer {mallet_inferencer} \
--doc-topics-threshold 0.005""".format(
        input_filename=input_filename,
        output_filename=output_filename, 
        mallet_inferencer=mallet_inferencer)
        command_seq = mallet_inference_command.split()
        process = subprocess.Popen(command_seq)
        process.communicate()
        
def get_canonical_vector(label):
    label_topic_dir = '/data/topics/individual_posts_{label}'.format(label=label)    
    cached_canonical_vector_filename = label_topic_dir + '/' + 'canonical_{}_topic_vector.out'.format(label)
    if os.path.isfile(cached_canonical_vector_filename):
        sys.stdout.write('\tReading in cached canonical topic vector for {}\n'.format(label))
        return joblib.load(cached_canonical_vector_filename)
    else:
        canonical_vector = np.zeros(2000)
        num_posts = 0
        document_topic_composition_filename = label_topic_dir + '/' + 'individual-posts-2000-composition.txt'
        topic_composition_file =  open(document_topic_composition_filename, 'r')
        for line in topic_composition_file:
            line = line.strip()
            if line.startswith('#'):
                continue
        
            num_posts += 1
            # parse out topic and proportion pairs
            # then parse out filename (which could contain whitespace)            
            topic_prop_pairs = re.findall('(?:\d+\s+0\.\d+)', line)
            for topic_prop_pair in topic_prop_pairs:
                vals = re.split('\s+', topic_prop_pair)
                topic_id = int(vals[0])
                proportion = float(vals[1])
                canonical_vector[topic_id] += proportion
        canonical_vector /= num_posts
        joblib.dump(canonical_vector, cached_canonical_vector_filename)
        return canonical_vector

def get_user_topic_vector(post_document_filename, document_to_topic_filename):
    post_file = open(post_document_filename, 'r')
    post_ids = [line.strip() for line in post_file]
    post_file.close()
    
    post_to_topic_probability = get_document_to_topic_proportions_dict_with_label_list(document_to_topic_filename, post_ids)
    
    user_to_posts = {}
    for post in post_to_topic_probability:
        user = post.split('_')[0]
        user_to_posts.setdefault(user, []).append(post)
        
    user_to_topic_vector = {}
    for user, posts in user_to_posts.items():
        user_to_topic_vector[user] = get_average_post_vector({post: post_to_topic_probability[post] for post in posts})
    return user_to_topic_vector
    
def get_average_post_vector(post_to_topic_probability):
    average_vector = np.zeros(2000)
    num_posts = len(post_to_topic_probability)
    for post in post_to_topic_probability:
        for topic, prob in post_to_topic_probability[post].items():
            average_vector[topic] += prob
    average_vector /= num_posts
    return average_vector
    
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

def weighted_distance(vec1, vec2, weights):
    dist = 0.0
    for v1, v2, w in zip(vec1, vec2, weights):
        if w > 1.0:
            dist += w*(v1 - v2)**2
    return np.sqrt(dist)

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

def get_largest_topic_differences(vector1, vector2, k=10):
    topic_to_top_words = get_topic_to_top_words()
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

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
    
    fbid = sys.argv[1]
    label = sys.argv[2]
    sys.stdout.write('Analyzing friend list for user {fbid} according to the \
distance from the canonical {label} user\n'.format(fbid=fbid, label=label))

    document_dir = '/data/user_documents/individual_posts_test_users'
    topic_dir = '/data/topics/individual_posts_test_users'    
    trained_topic_dir = '/data/topics/individual_posts_100000'

    conn = redshift_connect()
    # (1) Create a local file for the individual posts from all friends of fbid
    sys.stdout.write("Getting friends' posts...\n")
    post_document_filename = create_individual_post_document_file(conn, fbid, document_dir)
    if os.stat(post_document_filename).st_size == 0:
        sys.stdout.write("\tFound no friends' posts for {}\n".format(fbid))
        sys.exit()
    
    # (2) Create a mallet input file from the file containing all friend messages
    sys.stdout.write("Creating mallet input file...\n")
    mallet_input_filename = topic_dir + '/' + 'all-individual-posts-fbid-{}.mallet'.format(fbid)
    trained_mallet_input_filename = trained_topic_dir + '/' + 'all-individual-posts.mallet'
    import_mallet_file(post_document_filename, mallet_input_filename, trained_mallet_input_filename)
    
    # (3) Infer topics
    sys.stdout.write("Inferring topics on friends' posts...\n")
    document_to_topic_filename = topic_dir + '/' + 'individual-posts-fbid-{}-2000-composition.txt'.format(fbid)
    mallet_inferencer = trained_topic_dir + '/' + 'individual-posts-inferencer-2000.mallet'
    infer_mallet_topics(mallet_input_filename, document_to_topic_filename, mallet_inferencer)
    
    # (4) Load in canonical topic vector
    sys.stdout.write('Computing canonical topic vector for {} users...\n'.format(label))
    canonical_vector = get_canonical_vector(label)
    
    # (5) Get topic vector for each friend
    sys.stdout.write('Computing topic vector for each friend of {}...\n'.format(fbid))
    user_to_topic_vector = get_user_topic_vector(post_document_filename, document_to_topic_filename)
    
    # (6) Rank friends by distance to canonical topic vector
#     sort_users_by_canonical_distance(conn, user_to_topic_vector, canonical_vector)
    canonical_avg_vector = get_canonical_vector('100000')
#     sort_users_by_canonical_distance(conn, user_to_topic_vector, canonical_vector, distance=wminkowski, distance_kwargs={'p': 2, 'w': canonical_vector-canonical_avg_vector})
    get_largest_topic_differences(canonical_vector, canonical_avg_vector, k=10)
    sort_users_by_canonical_distance(conn, user_to_topic_vector, canonical_vector, distance=weighted_distance, distance_kwargs={'weights': canonical_vector/canonical_avg_vector})
#     sort_users_by_canonical_distance(conn, user_to_topic_vector, canonical_vector, distance=weighted_distance, distance_kwargs={'weights': np.maximum(canonical_vector/canonical_avg_vector, canonical_avg_vector/canonical_vector)})
    redshift_disconnect(conn)