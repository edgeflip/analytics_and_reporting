from db_utils import redshift_connect, redshift_disconnect, execute_query
import re

# data_dir = '/data/user_documents/individual_posts_100000'
data_dir = '/data/user_documents/individual_posts_vegan'

conn = redshift_connect()
query = """
        SELECT u.fbid as fbid, interests, movies, tv, music, quotes, sports
        FROM users u
            JOIN fbid_sample_vegans f
                ON u.fbid = f.fbid
        """
rows = execute_query(query, conn)
aboutme_document_filename = data_dir + '/' + 'all-individual-aboutme.txt'
file = open(aboutme_document_filename, 'w')
for row in rows:
    fbid = row[0]
    interests, movies, tv, music, quotes, sports = row[1:]
    if not interests:
        interests = ''
    if not movies:
        movies = ''
    if not tv:
        tv = ''
    if not music:
        music = ''
    if not quotes:
        quotes = ''
    if not sports:
        sports = ''
    message = ' '.join([interests, movies, tv, music, quotes, sports])
    message = ' '.join(re.split('\s+', message))
    
    file.write("{fbid} {message}\n".format(fbid=fbid, message=message))
file.close()
redshift_disconnect(conn)