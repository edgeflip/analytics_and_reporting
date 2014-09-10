import boto.dynamodb
import datetime
import psycopg2
import psycopg2.extras
import unicodecsv
from collections import defaultdict
from logging import debug, info, warning
from boto.s3.connection import S3Connection
import time
from cStringIO import StringIO
from redshift_utils import drop_table_if_exists

USER_COLUMNS = (
    'fbid',
    'birthday',
    'fname',
    'lname',
    'email',
    'gender',
    'city',
    'state',
    'country',
    'activities',
    'affiliations',
    'books',
    'devices',
    'friend_request_count',
    'has_timeline',
    'interests',
    'languages',
    'likes_count',
    'movies',
    'music',
    'political',
    'profile_update_time',
    'quotes',
    'relationship_status',
    'religion',
    'sports',
    'tv',
    'wall_count',
    'updated',
)

EDGE_COLUMNS =  (
	'fbid_source',
	'fbid_target', 
	'post_likes',
	'post_comms',
	'stat_likes',
	'stat_comms',
	'wall_posts',
	'wall_comms',
	'tags',
	'photos_target',
	'photos_other',
	'mut_friends',
	'updated',
)
print "in script"
MAX_STRINGLEN = 4096

def transform_field(field):
	string_representation = field
	if isinstance(field, set):
	    string_representation = str(list(field))
	if isinstance(field, list):
	    string_representation = str(field)
	if isinstance(string_representation, basestring):
            string_representation = string_representation.replace("\t", " ").replace("\n", " ").replace("\x00", "")
	    return string_representation[:MAX_STRINGLEN/2]

	return string_representation

print "defined function"
from keys import redshift
pconn = psycopg2.connect( **redshift)
pcur = pconn.cursor(cursor_factory = psycopg2.extras.DictCursor)


print "got redshift"
from keys import aws
dconn = boto.dynamodb.connect_to_region('us-east-1', **aws)
print "got dynamo"

def get_primaries(pcur):
    # distinct primaries missing from users
    pcur.execute("""
        SELECT DISTINCT(user_clients.fbid) AS fbid FROM users
        RIGHT JOIN user_clients using (fbid)
        WHERE 
	users.fname IS NULL or 
	(users.fname = ''AND updated < dateadd(week, -1, getdate()))
    """)
    fbids = [row['fbid'] for row in pcur.fetchall()]
    return fbids

def get_secondaries(pcur):
    pcur.execute("""
    	SELECT DISTINCT(edges.fbid_source) AS fbid FROM users
	RIGHT JOIN edges ON users.fbid=edges.fbid_source
	WHERE users.fname IS NULL or 
	(users.fname = '' AND users.updated < dateadd(week, -1, getdate()))
    """)
    fbids = [row['fbid'] for row in pcur.fetchall()]
    return fbids

def get_missing_edges(pcur):
	pcur.execute("""
	    SELECT DISTINCT users.fbid from users
	    LEFT JOIN visitors on (users.fbid = visitors.fbid)
	    LEFT JOIN user_clients on (users.fbid = user_clients.fbid)
	    LEFT JOIN missingedges on (missingedges.fbid = users.fbid)
	    LEFT JOIN edges on (edges.fbid_target = users.fbid)
	    WHERE
		edges.fbid_target is null
		AND missingedges.fbid is null
		AND COALESCE(visitors.fbid, user_clients.fbid) is not null
	    ORDER BY users.updated DESC
	    """, (4,))

	print "executed query"
	return [row['fbid'] for row in pcur.fetchall()]



def edges_to_key(fbids, dconn, bucket_name, key_name):
	stringfile = StringIO()                                                                                                                                 
	writer = unicodecsv.writer(stringfile, encoding='utf-8', delimiter="\t")
	i = 0
	found = 0
	not_found = 0
	table = dconn.get_table('prod.edges_incoming')
	for fbid in fbids:
	    i += 1
	    if i % 5 == 0:
		print "sleeping #", i, found, not_found, time.time()
		found = 0
		not_found = 0
		time.sleep(1)
	    print 'Seeking edge relationships for key', fbid
	    result = table.query(fbid)
	    if len(result.response['Items']) == 0:
		not_found += 1
	    else:
		found += 1
		info( "found {} edges from fbid {}".format( len(result.response['Items']), fbid))

	    for edge in result.response['Items']:
		d = defaultdict(lambda:0)
		d.update(edge)
		edge = d
		if 'updated' in edge and edge['updated']:
			edge['updated'] = datetime.datetime.fromtimestamp( edge['updated'])
		writer.writerow([transform_field(edge[field]) for field in EDGE_COLUMNS])

	s3conn = S3Connection(aws['aws_access_key_id'], aws['aws_secret_access_key'])
	bucket = s3conn.get_bucket(bucket_name) 
	key = bucket.new_key(key_name)
	stringfile.seek(0)
	key.set_contents_from_file(stringfile)


def fbids_to_key(fbids, dconn, bucketname, keyname):
	usertable = dconn.get_table('prod.users')
	stringfile = StringIO()                                                                                                                                 
	writer = unicodecsv.writer(stringfile, encoding='utf-8', delimiter="\t")
	for fbid in fbids:
		data = defaultdict(lambda: None)

		try:
		    debug('Seeking key {} in dynamo'.format(fbid))
		    dyndata = usertable.get_item(fbid)

		    # cast timestamps from seconds since epoch to dates and times
		    if 'birthday' in dyndata and dyndata['birthday']:
			dyndata['birthday'] = datetime.datetime.utcfromtimestamp( dyndata['birthday']).date()
		    elif 'birthday' in dyndata and dyndata['birthday'] == 0:
			del dyndata['birthday']

		    if 'profile_update_time' in dyndata and dyndata['profile_update_time']:
			dyndata['profile_update_time'] = datetime.datetime.utcfromtimestamp(dyndata['profile_update_time'])

		    if 'updated' in dyndata and dyndata['updated']:
			dyndata['updated'] = datetime.datetime.utcfromtimestamp( dyndata['updated'])
		    else:
			# some sort of blank row, track updated just to know when we went looking for it
			dyndata['updated'] = datetime.datetime.utcnow()

		    data.update(dyndata)

		except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
		    # this apparently is a real/possible thing, especially for legacy stuff
		    #warning('fbid {} not found in dynamo!'.format(fbid))

		    data['updated'] = datetime.datetime.now()
		writer.writerow([fbid if field == 'fbid' else transform_field(data[field]) for field in USER_COLUMNS])

	s3conn = S3Connection(aws['aws_access_key_id'], aws['aws_secret_access_key'])
	bucket = s3conn.get_bucket(bucketname) 
	key = bucket.new_key(keyname)
	stringfile.seek(0)
	key.set_contents_from_file(stringfile)


def load_users(connection, cursor, bucket_name, key_name, staging_table, final_table, access_key, secret_key):
	print "loading users"
	drop_table_if_exists(staging_table, connection, cursor)
	print "creating staging table"
	cursor.execute('create table {} (like {})'.format(staging_table, final_table))
	print "copying from s3 into staging table"
	copy_from_s3(connection, cursor, bucket_name, key_name, staging_table, access_key, secret_key)
	print "making room for new records"
	cursor.execute('delete from {} where fbid in (select distinct fbid from {})'.format(final_table, staging_table))
	print "inserting new records"
	cursor.execute('insert into {} select * from {}'.format(final_table, staging_table))
	connection.commit()
	

def copy_from_s3(connection, cursor, bucket_name, key_name, staging_table, access_key, secret_key):
    with connection:
        cursor.execute(
            """
            COPY {0} FROM 's3://{1}/{2}'
            CREDENTIALS 'aws_access_key_id={3};aws_secret_access_key={4}' delimiter '\t'
            """.format(staging_table, bucket_name, key_name, access_key, secret_key)
        )

if __name__ == '__main__':
	bucket = "warehouse-forklift"
	primaries = "primaries.csv"
	secondaries = "secondaries.csv"
	edges = "edges.csv"

	fbids = get_primaries(pcur)
	print "found ", len(fbids), "primaries"
	if len(fbids) > 0:
		fbids_to_key(fbids, dconn, bucket, primaries)
		load_users(pconn, pcur, bucket, primaries, 'users_staging', 'users', aws['aws_access_key_id'], aws['aws_secret_access_key'])

	fbids = get_missing_edges(pcur)
	print "found ", len(fbids), "users missing edges"
	if len(fbids) > 0:
		edges_to_key(fbids, dconn, bucket, edges)
		copy_from_s3(pconn, pcur, bucket, edges, 'edges',  aws['aws_access_key_id'], aws['aws_secret_access_key'])

	fbids = get_secondaries(pcur)
	print "found", len(fbids), "secondaries"
	if len(fbids) > 0:
		fbids_to_key(fbids, dconn, bucket, secondaries)
		load_users(pconn, pcur, bucket, secondaries, 'users_staging', 'users', aws['aws_access_key_id'], aws['aws_secret_access_key'])
