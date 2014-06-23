from db_utils import *
import os
import random

def get_user_from_friend_posts(sample_size, conn):
    '''
    Create a subset of the posts table that includes all posts from friends to users in
    the sampled fbid users.
    '''
    # need to group by user, post to deduplicate posts
    query = """
            CREATE TABLE fbid_sample_{sample_size}_from_friend_messages AS (
                SELECT fbid_user, fbid_post, min(message) as message
                FROM posts_compositekey
                    JOIN fbid_sample_{sample_size}
                        ON posts_compositekey.fbid_user=fbid_sample_{sample_size}.fbid
                WHERE fbid_user <> post_from
                GROUP BY fbid_user, fbid_post
            )
            """.format(sample_size=sample_size)
    execute_query(query, conn, fetchable=False)
    print('DONE!')
    
def create_user_from_friend_post_documents(data_dir, user_sample_file_name, sample_size, conn):
    '''
    For each sampled user, combine all messages of a given post type as a single document
    and save as a local file.
    '''
    file = open(user_sample_file_name, 'r')
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
                FROM fbid_sample_{sample_size}_from_friend_messages
                WHERE fbid_user = {fbid}
                """.format(fbid=fbid, sample_size=sample_size)
        document = ""
        rows = execute_query(query, conn)
        if rows:
            for row in rows:
                if row[0]:
                    document += row[0] + '\n'
        
        if not document: # no posts for the given user, then skip
            continue
        
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        user_from_friend_document_file_name = data_dir + '/' + '{fbid}_from_friend_messages.txt'.format(fbid=fbid)
        file = open(user_from_friend_document_file_name, 'w')
        file.write(document)
        file.close()

if __name__ == '__main__':
    ## Parameters ##
    sample_size = 50000
    data_dir = '/data/user_documents/all_from_friend_posts'
    user_sample_file_name = '/data/user_samples/user_sample_{}_with_birth_year_and_gender.tsv'.format(sample_size)

    ## Database operations ##
    conn = redshift_connect()
    print('Getting all from friend posts...')
    get_user_from_friend_posts(sample_size, conn)
    print('Creating user-message document files...')
    create_user_from_friend_post_documents(data_dir, user_sample_file_name, sample_size, conn)
    redshift_disconnect(conn)
