from logging import debug, info, warning, error
from time import strftime, time
from collections import defaultdict
import datetime

import boto.dynamodb
import MySQLdb
import MySQLdb.cursors
import psycopg2
import psycopg2.extras

from errors import mail_tracebacks
from redshift_utils import deploy_table, drop_table_if_exists

MAX_RETRIES = 4
MAX_STRINGLEN = 4096
OUR_IP_STRING = ','.join("'{}'".format(ip) for ip in ('38.88.227.194',))

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

INSERT_USER_QUERY = """
    INSERT INTO users
    ({})
    VALUES ({})
""".format(
    ','.join(USER_COLUMNS),
    ','.join(['%s'] * len(USER_COLUMNS))
)


class ETL(object):

    #queues of users to hit dynamo for
    new_fbids = set([])  # fbids in events/edges that aren't in the users table

    primary_fbids = set([])  # fbids in events that aren't in the users table yet
    secondary_fbids = set([])  # edges that aren't in the users table

    old_fbids = set([])  # fbids in the users table that we suspect have changed
    edge_fbids = set([]) # fbids in the users table that we don't have edge info for

    def connect_dynamo(self):
        # set up db connections, should put this in worker.py probably
        debug('Connecting to redshift..')
        from keys import redshift
        self.pconn = psycopg2.connect( **redshift)
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor)


        from keys import aws
        self.dconn = boto.dynamodb.connect_to_region('us-east-1', **aws)
        # This table gets used in a few places pretty steadily, "cache" it in here
        self.usertable = self.dconn.get_table('prod.users')


    def connect_rds(self):
        debug('Connecting to redshift..')
        from keys import redshift
        self.pconn = psycopg2.connect( **redshift)
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor)

        debug('Connecting to RDS..')
        from keys import rds
        self.mconn = MySQLdb.connect( cursorclass=MySQLdb.cursors.DictCursor, **rds)
        self.mcur = self.mconn.cursor()


    @mail_tracebacks
    def extract(self):
        """ main for syncing with RDS """
        from table_to_redshift import main as rds2rs

        self.mkstats()
        self.mkcampaignrollups()
        self.mkrollups()

        for table, table_id in [
            ('visits', 'visit_id'),
            ('visitors', 'visitor_id'),
            ('campaigns', 'campaign_id'),
            ('events', 'event_id'),
            ('clients', 'client_id'),
            ('campaign_properties', 'campaign_property_id'),
            ('user_clients', 'user_client_id'),
        ]:
            debug('Uploading {} ..'.format(table))
            rds2rs(table)
            debug('Done.')  # poor man's timer

        self.mkstats()
        self.mkcampaignrollups()
        self.mkrollups()
        info('RDS extraction complete, waiting for next run')


    def metric_expressions(self):
        return """
            COUNT(DISTINCT CASE WHEN t.type='incoming_redirect' THEN t.visit_id ELSE NULL END) AS visits,
            SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) AS clicks,
            COUNT(DISTINCT CASE WHEN t.type='authorized' THEN t.visit_id ELSE NULL END) AS authorized_visits,
            COUNT(DISTINCT fbid) AS uniq_users_authorized,
            SUM(CASE WHEN (t.type='auth_fail' or t.type='oauth_declined') THEN 1 ELSE 0 END) AS auth_fails,
            COUNT(DISTINCT CASE WHEN t.type='generated' THEN visit_id ELSE NULL END) AS visits_generated_faces,
            COUNT(DISTINCT CASE WHEN t.type='generated' THEN fbid ELSE NULL END) AS users_generated_faces,
            COUNT(DISTINCT CASE WHEN t.type='faces_page_rendered' THEN visit_id ELSE NULL END) AS visits_facepage_rendered,
            COUNT(DISTINCT CASE WHEN t.type='faces_page_rendered' THEN fbid ELSE NULL END) AS users_facepage_rendered,
            COUNT(DISTINCT CASE WHEN t.type='shown' THEN visit_id ELSE NULL END) AS visits_shown_faces,
            COUNT(DISTINCT CASE WHEN t.type='shown' THEN fbid ELSE NULL END) AS users_shown_faces,
            SUM(CASE WHEN t.type='shown' THEN 1 ELSE 0 END) AS total_faces_shown,
            COUNT(DISTINCT CASE WHEN t.type='shown' THEN t.friend_fbid ELSE NULL END) AS distinct_faces_shown,
            COUNT(DISTINCT CASE WHEN t.type='share_click' THEN t.visit_id ELSE NULL END) as visits_with_share_clicks,
            COUNT(DISTINCT CASE WHEN t.type='shared' THEN visit_id ELSE NULL END) AS visits_with_shares,
            COUNT(DISTINCT CASE WHEN t.type='shared' THEN fbid ELSE NULL END) AS users_who_shared,
            COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) AS audience,
            SUM(CASE WHEN t.type='shared' THEN 1 ELSE 0 END) AS total_shares,
            SUM(CASE WHEN t.type='clickback' THEN 1 ELSE 0 END) AS clickbacks
        """

    def mkstats(self):
        staging_table = 'clientstats_staging'
        drop_table_if_exists(staging_table, self.pconn, self.pcur)
        megaquery = """
    CREATE TABLE {} AS
    SELECT
        root_campaign.campaign_id,
        date_trunc('hour', t.updated) as hour,
        {}
        from events t
        inner join visits using (visit_id)
        inner join visitors v using (visitor_id)
        inner join campaigns using (campaign_id)
        inner join clients cl using (client_id)
        inner join campaign_properties using (campaign_id)
        inner join campaigns root_campaign on (root_campaign.campaign_id = campaign_properties.root_campaign_id)
        WHERE visits.ip not in ({})
        GROUP BY root_campaign.campaign_id, hour
        """.format(staging_table, self.metric_expressions(), OUR_IP_STRING)

        debug('Calculating client stats')
        with self.pconn:
            self.pcur.execute(megaquery)
        debug('beginning deploy table on clientstats')
        deploy_table('clientstats', 'clientstats_staging', 'clientstats_old', self.pcur, self.pconn)

        self.updated = strftime('%x %X')

        debug('Done.')


    def mkcampaignrollups(self):
        staging_table = 'campaignstats_staging'
        drop_table_if_exists(staging_table, self.pconn, self.pcur)
        megaquery = """
    CREATE TABLE {} AS
    SELECT
        root_campaign.campaign_id,
        {}
        from events t
        inner join visits using (visit_id)
        inner join visitors v using (visitor_id)
        inner join campaigns using (campaign_id)
        inner join clients cl using (client_id)
        inner join campaign_properties using (campaign_id)
        inner join campaigns root_campaign on (root_campaign.campaign_id = campaign_properties.root_campaign_id)
        WHERE visits.ip not in ({})
        GROUP BY root_campaign.campaign_id
        """.format(staging_table, self.metric_expressions(), OUR_IP_STRING)

        debug('Calculating campaign stats')
        with self.pconn:
            self.pcur.execute(megaquery)
        debug('beginning deploy table on campaignstats')
        deploy_table('campaignstats', staging_table, 'campaignstats_old', self.pcur, self.pconn)

        self.updated = strftime('%x %X')

        debug('Done.')


    def mkrollups(self):
        staging_table = 'clientrollups_staging'
        drop_table_if_exists(staging_table, self.pconn, self.pcur)
        megaquery = """
    CREATE TABLE {} AS
    SELECT
        client_id,
        {}
        from events t
        inner join visits using (visit_id)
        inner join visitors v using (visitor_id)
        inner join campaigns using (campaign_id)
        inner join clients cl using (client_id)
        WHERE visits.ip not in ({})
        GROUP BY client_id
        """.format(staging_table, self.metric_expressions(), OUR_IP_STRING)

        debug('Calculating client rollups')
        with self.pconn:
            self.pcur.execute(megaquery)
        debug('beginning deploy table on clientrollups')
        deploy_table('clientrollups', 'clientrollups_staging', 'clientrollups_old', self.pcur, self.pconn)

        self.updated = strftime('%x %X')

        debug('Done.')

    @mail_tracebacks
    def queue_users(self):
        t = time()

        # distinct primaries missing from users
        self.pcur.execute("""
            SELECT DISTINCT(user_clients.fbid) AS fbid FROM users
            RIGHT JOIN user_clients using (fbid)
            WHERE users.fbid IS NULL
        """)
        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        info("Found {} unknown primary fbids".format(len(fbids)))
        self.primary_fbids = self.primary_fbids.union(set(fbids))

        # secondaries that we don't have user records for
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
                INNER JOIN visitors ON users.fbid=visitors.fbid
                WHERE users.email IS NULL
                AND users.fname IS NOT NULL
                AND visitors.fbid IS NOT NULL
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


    def extract_user_primary(self):
        self.extract_user_batch(self.primary_fbids, 10)

    def extract_user_secondary(self):
        self.extract_user_batch(self.secondary_fbids, 100)


    @mail_tracebacks
    def extract_user_batch(self, collection, batch_size):
        self.batch_process(collection, batch_size, self.seek_user)


    @mail_tracebacks
    def extract_edge_batch(self):
        self.batch_process(self.edge_fbids, 10, self.extract_edge)


    def batch_process(self, collection, batch_size, procedure):
        batch = set()
        while collection and len(batch) < batch_size:
            batch.add(collection.pop())

        if len(batch) > 0:
            info('Created extraction batch for function {}. Contents: {}'.format(procedure, batch))

        with self.pconn:
            for fbid in batch:
                if fbid:
                    try:
                        procedure(fbid)
                    except StandardError as e:
                        # Complain, 'requeue', and fix the current transaction
                        # so the batch can proceed
                        warning('Error processing fbid {}: {}'.format(fbid, e))
                        collection.add(fbid)
                        self.pconn.commit()
        if len(batch) > 0:
            info('Batch complete')


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


    def transform_field(self, field):
        string_representation = None
        if isinstance(field, set):
            string_representation = str(list(field))
        if isinstance(field, list):
            string_representation = str(field)

        if isinstance(string_representation, str) and len(string_representation) > MAX_STRINGLEN:
            return string[:MAX_STRINGLEN]

        return string_representation


    def seek_user(self, fbid):

        data = defaultdict(lambda: None)

        try:
            debug('Seeking key {} in dynamo'.format(fbid))
            dyndata = self.usertable.get_item(fbid)

            # cast timestamps from seconds since epoch to dates and times
            if 'birthday' in dyndata and dyndata['birthday']:
                dyndata['birthday'] = datetime.datetime.utcfromtimestamp( dyndata['birthday']).date()

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
            warning('fbid {} not found in dynamo!'.format(fbid))

            data['updated'] = datetime.datetime.now()

        # insert whatever we got, even if it's a blank row
        self.pcur.execute(
            INSERT_USER_QUERY,
            [fbid if col == 'fbid' else self.transform_field(data[col]) for col in USER_COLUMNS]
        )


    @mail_tracebacks
    def queue_edges(self):
        # get primaries that don't have edges yet and we haven't given up on
        # TODO: remove visitors join once we fix user_clients to contain all primary fbids
        self.pcur.execute("""
            SELECT DISTINCT users.fbid from users
            LEFT JOIN visitors on (users.fbid = visitors.fbid)
            LEFT JOIN user_clients on (users.fbid = user_clients.fbid)
            LEFT JOIN missingedges on (
                missingedges.fbid = users.fbid and
                (missingedges.next_try is null or missingedges.retries >= %s or missingedges.next_try > getdate())
            )
            LEFT JOIN edges on (edges.fbid_target = users.fbid)
            WHERE
                edges.fbid_target is null
                AND missingedges.fbid is null
                AND COALESCE(visitors.fbid, user_clients.fbid) is not null
            ORDER BY users.updated DESC
            """, (MAX_RETRIES,))

        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        self.edge_fbids.update(fbids)

        info("{} fbids queued for edge extraction".format(len(self.edge_fbids)))



    @mail_tracebacks
    def extract_edge(self, fbid):
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
            self.pcur.execute("SELECT retries FROM missingedges WHERE fbid = %s", (fbid,))
            result = self.pcur.fetchone()
            # either schedule a retry with exponential backoff or give up
            if result and result[0] < MAX_RETRIES:
                self.pcur.execute("UPDATE missingedges set retries = retries + 1, next_try = dateadd(m, power(2, retries)::int, getdate()) where fbid = %s", (fbid,))
            elif not result:
                self.pcur.execute("INSERT INTO missingedges (fbid, retries, next_try) VALUES (%s, 1, getdate())", (fbid,))
            else:
                self.pcur.execute("UPDATE missingedges set next_try = null where fbid = %s", (fbid,))
            return

        info( 'Successfully updated edges table for fbid {}'.format(fbid))

