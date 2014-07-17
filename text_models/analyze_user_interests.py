import operator
import psycopg2

#### Helper methods to connect, disconnect, and query redshift ####
redshift = dict(
    host='analytics.cd5t1q8wfrkk.us-east-1.redshift.amazonaws.com',
    user='edgeflip',
    database='edgeflip',
    port=5439,
    password='XzriGDp2FfVy9K'
)

def redshift_connect():
    return psycopg2.connect(**redshift)

def redshift_disconnect(connection):
    connection.close()

def execute_query(query, connection, fetchable=True):
    cursor = connection.cursor()
    cursor.execute(query)
    if fetchable:
        rows = cursor.fetchall()
    connection.commit()
    cursor.close()
    if fetchable:
        return rows
###################################################################

# Initialize counters and data structures
num_users = 0
interest_to_count = {} # dictionary of 'interest' -> 'number of users with that interest'
num_interests_to_count = {} # dictionary of 'number of interests' -> 'number of users who have that many interests'

# Get data from redshift
conn = redshift_connect()
query = """
        SELECT fbid, min(interests)
        FROM users
        GROUP BY fbid
        """ # group by fbid because there still are some duplicates
rows = execute_query(query, conn)#, iterable=True)

# Example interest to parse: 
# [u'Bar trivia', u'Board Games', u'Card Games', u'Chocolate Ice Cream']
# Note: could eval as a list of strings, but some entries get cut off if num chars > 4096
# so we'll find the last acceptable position in the string and take on a ']'

for row in rows:    
    fbid = row[0]
    interests_raw = row[1]
    num_users += 1
    if num_users % 100000 == 0:
        print(num_users)
    if interests_raw: # if this user has an entry for interests
        # converting all interests to lower case
        interests_raw = interests_raw.lower()
        # if interests were cut off in the middle of an interest, chop to last complete one
        if interests_raw[-2:] not in ['\']', '"]']:
            interests_raw = interests_raw[:interests_raw.rfind('\',')]
            interests_raw += '\']'
        interests = eval(interests_raw)
        for interest in interests: # for each interest, keep increment its user count
            interest_to_count.setdefault(interest, 0)
            interest_to_count[interest] += 1
        # keep track of total number of interests for each user
        num_interests_to_count.setdefault(len(interests), 0)
        num_interests_to_count[len(interests)] += 1
    else: # if there are no interests, we need to increment the zero count
        num_interests_to_count.setdefault(0, 0)
        num_interests_to_count[0] += 1
redshift_disconnect(conn)

# Sort both dictionaries in descending order according to counts
sorted_interest_counts = sorted(interest_to_count.iteritems(), key=operator.itemgetter(1), reverse=True)
sorted_num_interests_to_count = sorted(num_interests_to_count.iteritems(), key=operator.itemgetter(1), reverse=True)

# Save interest_to_count and num_interests_to_count to local files
# Change filenames to wherever you want to save them. 
# This defaults to the directory you're executing from.
file = open('interest-to-count.tsv', 'w')
for interest, count in sorted_interest_counts:
    file.write('{}\t{}\n'.format(interest.encode('utf-8'), count))
file.close()

file = open('num-interests-to-count.tsv', 'w')
for num_interests, count in sorted_num_interests_to_count:
    file.write('{}\t{}\n'.format(num_interests, count))
file.close()

# Print out some useful statistics
print('Num users: {}'.format(num_users))
print('Num interests: {}'.format(len(interest_to_count)))

print('Top 10 interests:')
for i in range(10):
    print('{}\t{}'.format(sorted_interest_counts[i][0], sorted_interest_counts[i][1]))

print('Top 10 number of interests:')
for i in range(10):
    print('{}\t{}'.format(sorted_num_interests_to_count[i][0], sorted_num_interests_to_count[i][1])