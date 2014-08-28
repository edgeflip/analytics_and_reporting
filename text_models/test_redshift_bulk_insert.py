from db_utils import redshift_connect, redshift_disconnect, execute_query
from cStringIO import StringIO
from boto.s3.connection import S3Connection

aws_access_key_id='AKIAJMCUUDDXI5EXTENA'
aws_secret_access_key='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'

s3conn = S3Connection(aws_access_key_id, aws_secret_access_key)
bucket = s3conn.get_bucket('marc_temp_batches')
key = bucket.new_key('batch_test.tsv')

f = StringIO()
f.write('1\t10.5\n2\t3.590234\n')
f.write('6890\t-0.12350\n')
f.write('73\t\N\n')
f.seek(0)

key.set_contents_from_file(f)

conn = redshift_connect()
query = 'create table marc_test_insert (v1 int, v2 double precision)'
execute_query(query, conn, fetchable=False)

query = """
            COPY marc_test_insert FROM 's3://marc_temp_batches/batch_test.tsv'
            CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}' delimiter '\t'
            """.format(aws_access_key_id, aws_secret_access_key)
execute_query(query, conn, fetchable=False)

redshift_disconnect(conn)