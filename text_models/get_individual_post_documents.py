from db_utils import redshift_connect, redshift_disconnect, execute_query
import re
import sys

label = sys.argv[1]
data_dir = '/data/user_documents/individual_posts_{}'.format(label)

conn = redshift_connect()
query = """
        SELECT fbid_post, message
        FROM 
            (SELECT fbid, fbid_post, MIN(message) AS message
            FROM fbid_sample_{label} f
                JOIN posts_raw p ON f.fbid = p.fbid_user
            WHERE p.fbid_user = p.post_from
                 AND message <> ''
            GROUP BY fbid, fbid_post) t            
        """.format(label=label)

rows = execute_query(query, conn)
post_document_filename = data_dir + '/' + 'all-individual-posts.txt'
file = open(post_document_filename, 'w')
for row in rows:
    fbid_post, message = row
    message = ' '.join(re.split('\s+', message))    
    file.write("{fbid_post} {message}\n".format(fbid_post=fbid_post, message=message))
file.close()
redshift_disconnect(conn)

