from logging import debug, info, warning, error
import cStringIO
import csv
from time import strftime, time
from collections import defaultdict
import datetime
import functools

from boto.s3.connection import S3Connection
import boto.dynamodb
import MySQLdb
import MySQLdb.cursors
import psycopg2
import psycopg2.extras

from tornado.web import HTTPError
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web


def mail_tracebacks(method):
    """
    Decorator to forward tracebacks on to concerned parties
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        #check for debug mode
        try:
            return method(self, *args, **kwargs)
        except:
            import traceback
            traceback.print_exc()

            # if debug mode is off, email the stack trace
            from tornado.options import options
            if options.debug: raise

            err = traceback.format_exc()

            import email
            msg = email.Message.Message()
            msg['Subject'] = 'UNHANDLED EXCEPTION'
            msg.set_payload(err)

            import smtplib
            smtp = smtplib.SMTP()
            smtp.connect()
            smtp.sendmail('error@edgeflip.com', ['japhy@edgeflip.com',], msg.as_string())

    return wrapper


def mkCSV(application, t=False, client_id=2):
    """ Grab event data for the hour preceding t """
    rs = application.pcur  # redshift cursor, need better naming convention

    # ok.. if i put this in the actual query, Redshift says it's not supported
    rs.execute("SELECT DATE_TRUNC('hour', now()) - INTERVAL '1 hour'")
    hour = rs.fetchone()[0]
    info('Gathering datapoints for client {}, hour {}'.format(client_id, str(hour)))


    # S3 connection, base filename
    from keys import aws
    S3 = S3Connection( **aws)
    bucket = S3.get_bucket('ef-client-data')
    basefile = "{}-{}-{}-{}-{}".format('virginia',hour.year,hour.month,hour.day,hour.hour)


    # EVENTS
    # get campaign_id, activity_id
    rs.execute( """    
    SELECT e.event_datetime AS time, v.session_id, v.fbid, e.friend_fbid, e.type, root.root_id, e.activity_id
    FROM events AS e, visits AS v, campchain as root 
        WHERE e.visit_id=v.visit_id 
        AND DATE_TRUNC('hour', time) = %s
        AND e.campaign_id IN
        (SELECT DISTINCT(campaign_id) FROM campaigns WHERE client_id=%s)
        AND e.type IN ('session_start', 'authorized', 'shared', 'clickback') 
        AND root.parent_id IS NOT NULL AND e.campaign_id=root.parent_id
    ORDER BY time DESC 
    """, (hour,client_id))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['time', 'session_id', 'fbid', 'friend_fbid','type', 'campaign_id', 'activity_id']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        debug(row)
        writer.writerow(row)

    # put it on S3
    f.seek(0)
    key = bucket.new_key(basefile + '-events.csv')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()  # clear memory

    # USERS
    rs.execute( """
    SELECT fbid,fname,lname,gender,city,state,birthday,email
    FROM users
    WHERE fbid IN
        (SELECT DISTINCT(fbid) FROM visits, events
        WHERE visits.visit_id=events.visit_id
            AND date_trunc('hour', events.event_datetime) = %s
            AND fbid IS NOT NULL
            AND events.campaign_id IN
            (SELECT DISTINCT(campaign_id) FROM campaigns WHERE client_id=%s)
        )
    """, (hour,client_id))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['fbid', 'fname', 'lname', 'gender','city','state','birthday','email']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        writer.writerow(row)

    # put it on S3
    f.seek(0)
    key = bucket.new_key(basefile + '-users.csv')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()

    # Params for Virginia's service
    import requests
    import json
    out = {'bucket':'ef-client-data'}
    out['type'] = 'EventFile'
    out['key'] = basefile+'-events.csv'

    headers = {'Content-Type': 'application/json'}

    requests.post('http://va-c2v.herokuapp.com/datafiles', data=json.dumps(out), headers=headers)
    out['type'] = 'UserFile'
    out['key'] = basefile+'-users.csv'
    requests.post('http://va-c2v.herokuapp.com/datafiles', data=json.dumps(out), headers=headers)

    import email
    msg = email.Message.Message()
    msg['Subject'] = 'Uploaded {} for VA'.format(basefile)

    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect()
    smtp.sendmail('japhy@edgeflip.com', ['japhy@edgeflip.com',], msg.as_string())


"""
A CSV of all of a client's email addresses.. a one off for VA that
in theory they wanted run multiple times, but they seem to have forgotten
about it
"""
def mkemailCSV(application, client_id=2):
    info('Making email CSV for client {}'.format(client_id))

    rs = application.pcur  # redshift cursor, need better naming convention

    # S3 connection, base filename
    from keys import aws
    S3 = S3Connection( **aws)
    bucket = S3.get_bucket('ef-client-data')

    rs.execute("""
        SELECT DISTINCT fname,lname,email 
        FROM users,visits,events,campaigns 
        WHERE users.fbid=visits.fbid 
            AND visits.visit_id=events.visit_id 
            AND events.campaign_id=campaigns.campaign_id 
            AND campaigns.client_id=%s
        """,(client_id,))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['fname','lname','email']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        debug(row)
        writer.writerow(row)


    # put it on S3
    f.seek(0)
    key = bucket.new_key('54e946104e477c00df6fd684e0955d7e')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()

    info('Done.')

"""
Make a CSV of all of VA's campaign stats, grouped by `source`
"""
def mkSumEmail(application, client_id=2):

    rs = application.pcur  # redshift cursor, need better naming convention
    rs.execute("""
    SELECT
        root_id,
        campaigns.name,
        source,
        SUM(CASE WHEN t.type='button_load' THEN 1 ELSE 0 END) AS visits,
        SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN t.type='authorized' THEN 1 ELSE 0 END) AS auths,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN fbid ELSE NULL END) AS shown,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN fbid ELSE NULL END) AS shares,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) AS audience,
        COUNT(DISTINCT CASE WHEN t.type='clickback' THEN t.cb_visit_id ELSE NULL END) AS clickbacks
    FROM
        (
            SELECT e1.visit_id,
                e1.campaign_id,
                e1.content_id,
                e1.friend_fbid,
                e1.type,
                e1.content,
                e1.activity_id,
                NULL AS cb_visit_id,
                e1.updated
            FROM events e1
                WHERE type <> 'clickback'
            UNION
            (
            SELECT e3.visit_id,
                e3.campaign_id,
                e2.content_id,
                e3.friend_fbid,
                e2.type,
                e2.content,
                e2.activity_id,
                e2.visit_id AS cb_visit_id,
                e2.updated
            FROM events e2
            LEFT JOIN events e3 USING (activity_id)
                WHERE e2.type='clickback' AND e3.type='shared'
            )
        ) t
    INNER JOIN (SELECT fbid, source, visit_id FROM visits) v
        USING (visit_id)
    INNER JOIN campchain ON parent_id=campaign_id
    INNER JOIN campaigns ON campchain.root_id=campaigns.campaign_id
    WHERE campaigns.client_id=%s
    GROUP BY root_id, source, campaigns.name
    ORDER BY root_id DESC
    """, (client_id,))


    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['campaign_id','name','source', 'visits', 'clicks', 'uniq_auths', 'faces_shown', 'shares', 'audience', 'clickbacks']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        debug(row)
        writer.writerow(row)

    import email
    msg = email.Message.Message()
    msg['Subject'] = 'Campaign Statistics CSV - {}'.format(datetime.date.today().isoformat())

    msg.set_payload(f.getvalue())

    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect()
    smtp.sendmail('japhy@edgeflip.com', ['japhy@edgeflip.com',
                                        'alex@terrymcauliffe.com',
                                        'dbutterfield@bpimedia.com',
                                        'aelman@bpimedia.com',
                                        ], msg.as_string())


"""
one off to make a CSV from a dynamo table
"""
def mkdynemail(application, client_id=2):

    rs = application.pcur  # redshift cursor, need better naming convention
    rs.execute("""
    SELECT * from dyn_tokens
    """, )


    # then make some csvs
    f = cStringIO.StringIO()
    writer = csv.writer(f, delimiter=",")
    for row in rs.fetchall():
        debug(row)
        writer.writerow(row)

    # put it on S3
    from keys import aws
    S3 = S3Connection( **aws)
    bucket = S3.get_bucket('ef-client-data')
    f.seek(0)
    key = bucket.new_key('dyndump')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()



class ETL(object):

    #queues of users to hit dynamo for
    new_fbids = set([])  # fbids in events/edges that aren't in the users table

    primary_fbids = set([])  # fbids in events that aren't in the users table yet
    secondary_fbids = set([])  # edges that aren't in the users table

    old_fbids = set([])  # fbids in the users table that we suspect have changed
    edge_fbids = set([]) # fbids in the users table that we don't have edge info for

    def connect(self):
        # set up db connections, should put this in worker.py probably

        """
        I think that long running transactions cause:
        # OperationalError: SSL SYSCALL error: EOF detected
        eg, we should rebuild this cursor more often, or commit() every so often
        """
        debug('Connecting to redshift..')
        from keys import redshift
        self.pconn = psycopg2.connect( **redshift)
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 

        debug('Connecting to RDS..')
        from keys import rds
        self.mconn = MySQLdb.connect( cursorclass=MySQLdb.cursors.DictCursor, **rds)
        self.mcur = self.mconn.cursor()

        from keys import aws
        self.dconn = boto.dynamodb.connect_to_region('us-east-1', **aws)
        # This table gets used in a few places pretty steadily, "cache" it in here
        self.usertable = self.dconn.get_table('prod.users')
 
        debug('Done.')

    @mail_tracebacks
    def extract(self):
        """ main for syncing with RDS """
        from table_to_redshift import main as rds2rs

        self.mkchains()

        for table, table_id in [
            ('visits', 'visit_id'), 
            ('campaigns', 'campaign_id'),
            ('events', 'event_id'),
            ('clients', 'client_id'),
            ('campaign_properties', 'campaign_property_id'),
            ('clientstats', False),
            ]:
            try:
                debug('Dropping table _{}'.format(table))
                self.pcur.execute( "DROP TABLE _{}".format(table))
                self.pconn.commit()
            except Exception as e:
                # usually just "table doesn't exist"
                warning( '{}'.format(e))
                self.pconn.rollback()

            if not table_id: continue

            try:
                debug('Uploading {} ..'.format(table))
                rds2rs(table, self.pcur)
                debug('Done.')  # poor man's timer

                self.pcur.execute( """
                    INSERT INTO {table} SELECT * FROM _{table} 
                    WHERE {table_id} > (SELECT max({table_id}) FROM {table})
                    """.format(table=table, table_id=table_id))
                self.pconn.commit()
            except Exception as e:
                warning( '{}'.format(e))
                self.pconn.rollback()
                raise

        self.mkchains()
        self.mkstats()


    def mkchains(self):
        """
        calculate campaign relationships via the _properties table, be sure to run this
        after campaigns have been loaded to avoid race conditions
        """

        self.mcur.execute( """
        SELECT t1.campaign_id 
        FROM campaign_properties AS t1 
            LEFT JOIN campaign_properties AS t2 ON t1.campaign_id=t2.fallback_campaign_id 
        WHERE t2.fallback_campaign_id IS NULL;
        """)

        roots = [row['campaign_id'] for row in self.mcur.fetchall()]

        for root_id in roots:
            debug('Wiping root {}'.format(root_id))

            #wipe anything that's in there
            self.pcur.execute( """DELETE FROM campchain WHERE root_id=%s""", (root_id,))

            self.pcur.execute("""
            SELECT fallback_campaign_id FROM campaign_properties WHERE campaign_id=%s
            """, (root_id,))
            fallback_id = self.pcur.fetchone()['fallback_campaign_id']

            # create row for root:
            if not fallback_id:
                self.pcur.execute("""
                INSERT INTO campchain VALUES (%s,%s,%s)
                """, (root_id, root_id, fallback_id))

            parent_id = root_id 
            while fallback_id:
                # crawl down the chain

                self.pcur.execute("""
                SELECT fallback_campaign_id FROM campaign_properties WHERE campaign_id=%s
                """, (parent_id,))
                fallback_id = self.pcur.fetchone()['fallback_campaign_id']

                debug('crawling root {}, on child {}'.format(root_id, fallback_id))

                self.pcur.execute("""
                INSERT INTO campchain VALUES (%s,%s,%s)
                """, (root_id, parent_id, fallback_id))

                parent_id = fallback_id

            # commit each set of root updates in a single transaction
            self.pconn.commit()


    def mkstats(self):
        megaquery = """
    CREATE TABLE _clientstats AS
    SELECT
        t.campaign_id,
        date_trunc('hour', t.updated) as hour,
        SUM(CASE WHEN t.type='button_load' THEN 1 ELSE 0 END) AS visits,
        SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN t.type='authorized' THEN 1 ELSE 0 END) AS auths,
        COUNT(DISTINCT CASE WHEN t.type='authorized' THEN fbid ELSE NULL END) AS uniq_auths,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN fbid ELSE NULL END) AS shown,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN fbid ELSE NULL END) AS shares,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) AS audience,
        COUNT(DISTINCT CASE WHEN t.type='clickback' THEN t.cb_visit_id ELSE NULL END) AS clickbacks

    FROM
        (
            SELECT e1.visit_id, 
                e1.campaign_id, 
                e1.content_id, 
                e1.friend_fbid,
                e1.type, 
                e1.content, 
                e1.activity_id, 
                NULL AS cb_visit_id, 
                e1.updated
            FROM events e1
                WHERE type <> 'clickback'
            UNION
            (
            SELECT e3.visit_id,
                e3.campaign_id,
                e2.content_id,
                e3.friend_fbid,
                e2.type,
                e2.content,
                e2.activity_id,
                e2.visit_id AS cb_visit_id,
                e2.updated
            FROM events e2 
            LEFT JOIN events e3 USING (activity_id)
                WHERE e2.type='clickback' AND e3.type='shared'
            )
        ) t

    INNER JOIN (SELECT fbid, visit_id FROM visits) v
        USING (visit_id)
    GROUP BY t.campaign_id, hour
        """

        debug('Calculating client stats')
        self.pcur.execute(megaquery)
        self.pconn.commit()
        debug( 'beginning DELETE / INSERT on clientstats') 
        self.pcur.execute("DELETE FROM clientstats WHERE 1")
        self.pcur.execute("INSERT INTO clientstats SELECT * FROM _clientstats WHERE 1")
        self.pconn.commit()

        self.updated = strftime('%x %X')

        debug('Done.')


    @mail_tracebacks
    def queue_users(self):
        t = time()

        # distinct fbids from visits missing from users
        # union: secondaries that we don't have user records for
        self.pcur.execute("""
            SELECT DISTINCT(visits.fbid) AS fbid FROM users 
                RIGHT JOIN visits 
                    ON visits.fbid=users.fbid 
                WHERE users.fbid IS NULL 
            """)
        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        info("Found {} unknown primary fbids".format(len(fbids)))
        self.primary_fbids = self.primary_fbids.union(set(fbids))

        self.pcur.execute("""
            SELECT DISTINCT(edges.fbid_source) AS fbid FROM users
                RIGHT JOIN edges
                    ON users.fbid=edges.fbid_source
                WHERE users.fbid IS NULL
            """)

        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        info("Found {} unknown secondary fbids".format(len(fbids)))
        self.secondary_fbids = self.secondary_fbids.union(set(fbids))

        # probably missing, but potentially we need to scan for this user again
        self.pcur.execute("""
            SELECT DISTINCT(users.fbid) FROM users
                INNER JOIN visits ON users.fbid=visits.fbid
                WHERE users.email IS NULL
                AND users.fname IS NOT NULL
                AND visits.fbid IS NOT NULL
            """)

        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        info("Found {} blank fbids".format(len(fbids)))
        self.old_fbids = self.old_fbids.union(set(fbids))

        info( 'Queued users for extraction in {}'.format(time()-t))


    @mail_tracebacks
    def extract_user(self):
        """ Grab a fbid off of the queue and get it out of dynamo """
        if len(self.primary_fbids) < 1 and len(self.secondary_fbids) < 1: return
        fbid = self.primary_fbids.pop() if len(self.primary_fbids) > 0 else self.secondary_fbids.pop()

        # null fbids :\
        if not fbid: return

        self.seek_user(fbid)

        self.pconn.commit()
        info( 'Extracted fbid {} from Dynamo'.format(fbid))


    @mail_tracebacks
    def refresh_user(self):
        """ Take a fbid (probably blank) and check that it looks good """
        if len(self.old_fbids) < 1: return
        fbid = self.old_fbids.pop()
        if not fbid: return  # null fbids :\

        # wipe out the old row, we'll insert a new (blank) even if we find nothing
        self.pcur.execute("""
            DELETE FROM users WHERE fbid=%s
            """, (fbid,))

        self.seek_user(fbid)

        self.pconn.commit()
        info( 'Updated fbid {} from Dynamo'.format(fbid))


    def seek_user(self, fbid):

        data = defaultdict(lambda: None)

        try:
            debug('Seeking key {} in dynamo'.format(fbid))
            dyndata = self.usertable.get_item(fbid)

            # cast timestamps from seconds since epoch to dates and times
            if 'birthday' in dyndata and dyndata['birthday']:
                dyndata['birthday'] = datetime.date.fromtimestamp( dyndata['birthday'])
            else: 
                dyndata['birthday'] = None

            if 'updated' in dyndata and dyndata['updated']:
                dyndata['updated'] = datetime.datetime.fromtimestamp( dyndata['updated'])
            else:
                # some sort of blank row, track updated just to know when we went looking for it
                dyndata['updated'] = datetime.datetime.now()

            data.update(dyndata)

        except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
            # this apparently is a real/possible thing, especially for legacy stuff
            warning('fbid {} not found in dynamo!'.format(fbid))

            data['updated'] = datetime.datetime.now()

        # insert whatever we got, even if it's a blank row
        self.pcur.execute("""
            INSERT INTO users
            (fbid, fname, lname, email, gender, birthday, city, state, updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (fbid, data['fname'], data['lname'], data['email'], 
                    data['gender'], data['birthday'], data['city'], data['state'], 
                    data['updated'])
            )

        return data


    @mail_tracebacks
    def queue_edges(self):
        self.pcur.execute("""
            SELECT DISTINCT fbid FROM visits 
            WHERE fbid NOT IN (SELECT DISTINCT fbid_target FROM edges)
            AND fbid NOT IN (SELECT DISTINCT fbid FROM missingedges)
            ORDER BY updated DESC
            """)

        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        self.edge_fbids = self.edge_fbids.union( set(fbids))

        info("{} fbids queued for edge extraction".format(len(self.edge_fbids)))

    @mail_tracebacks
    def extract_edge(self):
        if len(self.edge_fbids) < 1: return
        fbid = self.edge_fbids.pop()
        # EDGE DATA
        table = self.dconn.get_table('prod.edges_incoming')
        try:
            debug('Seeking edge relationships for key {}'.format(fbid))
            result = table.query(fbid)
            if len(result.response['Items']) == 0:
                raise boto.dynamodb.exceptions.DynamoDBKeyNotFoundError('Empty set returned for {}'.format(fbid))
            else:
                info( "found {} edges from fbid {}".format( len(result.response['Items']), fbid))

            edges = []
            for edge in result.response['Items']:
                """ 
                some, relatively rare, dynamo records only have px3 data, so wall_comms, post_comms, etc
                are missing..  the workaround is to make a defaultdict with 0s ? tho maybe NULL would be better
                """
                d = defaultdict(lambda:0)
                d.update(edge)
                edge = d
                edges.append( "({},{},{},{},{},{},{},{},{},{},{},{},'{}')".format(
                            edge['fbid_source'], edge['fbid_target'], edge['wall_comms'], edge['post_comms'], 
                            edge['tags'], edge['wall_posts'], edge['mut_friends'], edge['stat_likes'], 
                            edge['photos_other'], edge['post_likes'], edge['photos_target'], edge['stat_comms'], 
                            datetime.datetime.fromtimestamp( edge['updated'])) )

            # insert what we got
            self.pcur.execute("""
                    INSERT INTO edges
                    (fbid_source, fbid_target, wall_comms, post_comms, 
                        tags, wall_posts, mut_friends, stat_likes, 
                        photos_other, post_likes, photos_target, stat_comms, 
                        updated )
                    VALUES 
                    """ + ",".join(edges),
                    )



        except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
            warning('fbid {} not found in dynamo edges'.format(fbid))
            self.pcur.execute("INSERT INTO missingedges (fbid) VALUES (%s)", (fbid,))
            self.pconn.commit()
            return 

        info( 'Successfully updated edges table for fbid {}'.format(fbid))
        self.pconn.commit()

