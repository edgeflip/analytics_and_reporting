from db_utils import *
import os
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

def get_user_sample_with_birth_year_and_gender(sample_size, data_dir, output_file_name, connnection):
    '''	
    Get sample_size random fb users, either sources (secondary) or targets (primary), 
    that have an observed value for birthday.
    Stores a table in redshift and saves a local file (that adds a column for age group)
    '''
    query = """
            CREATE TABLE fbid_sample_{sample_size} AS (
                SELECT fbid,
                       extract(year from min(birthday)) birth_year,
                       min(gender)
                FROM users
                WHERE birthday is not null and gender is not null
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
        fbid, birth_year, gender = row
        gender = 'male' if gender.startswith('male') else 'female'
        age_group = birth_year_to_age_range(birth_year)
        output_file.write('{}\t{}\t{}\t{}\n'.format(fbid, birth_year, age_group, gender))
    output_file.close()

def get_user_posts_by_type(sample_size, conn):
    '''
    Create a subset of the posts table that includes all posts of type post_type posted by 
    the sampled fbid users.
    '''
    # need to group by user, post to deduplicate posts
#     query = """
#             CREATE TABLE fbid_sample_{sample_size}_posts_{post_type} AS (
#                 SELECT fbid_user, fbid_post, min(message) as message
#                 FROM posts
#                     JOIN fbid_sample_{sample_size} 
#                         ON posts.fbid_user=fbid_sample_{sample_size}.fbid
#                 WHERE type = '{post_type}'
#                 GROUP BY fbid_user, fbid_post
#             )
#             """.format(sample_size=sample_size, post_type=post_type)
    query = """
            CREATE TABLE fbid_sample_{sample_size}_messages AS (
                SELECT fbid_user, fbid_post, min(message) as message
                FROM posts
                    JOIN fbid_sample_{sample_size} 
                        ON posts.fbid_user=fbid_sample_{sample_size}.fbid
                WHERE fbid_user = post_from
                GROUP BY fbid_user, fbid_post
            )
            """.format(sample_size=sample_size)
    execute_query(query, conn, fetchable=False)
    print('DONE!')
    
def create_user_post_type_documents(data_dir, user_sample_file_name, sample_size, conn):
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
                FROM fbid_sample_{sample_size}_messages
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
        user_document_file_name = data_dir + '/' + '{fbid}_messages.txt'.format(fbid=fbid)
        file = open(user_document_file_name, 'w')
        file.write(document)
        file.close()

if __name__ == '__main__':
    ## Parameters ##
    sample_size = 50000
    data_dir = '/data/user_documents/all_originating_posts'
    user_sample_file_name = '/data/user_samples/user_sample_{}_with_birth_year_and_gender.tsv'.format(sample_size)
    post_type = 'status'

    ## Database operations ##
    conn = redshift_connect()
    # print('Getting {} random users with birth years and genders...'.format(sample_size))
    # get_user_sample_with_birth_year_and_gender(sample_size, data_dir, user_sample_file_name, conn)
    # print('Getting all their posts...')
    # get_user_posts_by_type(sample_size, conn)
    print('Creating user-message document files...')
    create_user_post_type_documents(data_dir, user_sample_file_name, sample_size, conn)
    redshift_disconnect(conn)
