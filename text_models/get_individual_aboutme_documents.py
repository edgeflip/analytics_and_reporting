from db_utils import redshift_connect, redshift_disconnect, execute_query
import re
import sys

label = sys.argv[1]
data_dir = '/data/user_documents/individual_posts_{}'.format(label)

conn = redshift_connect()
query = """
        SELECT u.fbid as fbid, min(books) as books, min(interests) as interests, 
               min(movies) as movies, min(tv) as tv, min(music) as music, 
               min(quotes) as quotes, min(sports) as sports
        FROM fbid_sample_{} f
            LEFT JOIN users u
                ON u.fbid = f.fbid
        GROUP BY u.fbid
        """.format(label)
rows = execute_query(query, conn)
aboutme_document_filename = data_dir + '/' + 'all-individual-aboutme.txt'
file = open(aboutme_document_filename, 'w')
for row in rows:
    fbid = row[0]
    books, interests, movies, tv, music, quotes, sports = row[1:]
#     interests, quotes = row[1:]
    if not books:
        books = ''
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
    message = ' '.join([books, interests, movies, tv, music, quotes, sports])
#     message = ' '.join([interests, quotes])
    message = ' '.join(re.split('\s+', message))
    
    file.write("{fbid} {message}\n".format(fbid=fbid, message=message))
file.close()
redshift_disconnect(conn)

