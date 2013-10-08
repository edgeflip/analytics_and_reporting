from logging import debug, info, warning
import json
import datetime
import MySQLdb
from time import strftime, time
from collections import defaultdict

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import psycopg2
import psycopg2.extras
import boto.dynamodb
from tornado.web import HTTPError

from auth import Login, Logout, AuthMixin

class ETL(object):

    #queues of users to hit dynamo for
    fbids = set([])
    edge_fbids = set([])

    def connect(self):
        """make db connections, would be cool to time this out"""

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
        self.mconn = MySQLdb.connect( **rds)

        from keys import aws
        self.dconn = boto.dynamodb.connect_to_region('us-east-1', **aws)
 
        debug('Done.')

    def extract(self):
        from table_to_redshift import main as rds2rs

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
                raise
                warning( '{}'.format(e))
                self.pconn.rollback()

        self.queue_users()
        self.queue_edges()
        self.mkchains()
        self.mkstats()


    def mkchains(self):
        """
        calculate campaign relationships via the _properties table, be sure to run this
        after campaigns have been loaded to avoid race conditions
        """

        self.pcur.execute( """
            SELECT campaign_id, name FROM campaigns WHERE campaign_id IN
                (SELECT DISTINCT(campaign_id) FROM events WHERE type='button_load')
            ORDER BY campaign_id DESC
            """)
        roots = [row['campaign_id'] for row in self.pcur.fetchall()]

        for root_id in roots:
            debug('Wiping root {}'.format(root_id))

            #wipe anything that's in there
            self.pcur.execute( """DELETE FROM campchain WHERE root_id=%s""", (root_id,))

            self.pcur.execute("""
            SELECT fallback_campaign_id FROM campaign_properties WHERE campaign_id=%s
            """, (root_id,))
            fallback_id = self.pcur.fetchone()['fallback_campaign_id']

            # create row for root:
            self.pcur.execute("""
            INSERT INTO campchain VALUES (%s,%s,%s)
            """, (root_id, None, fallback_id))

            parent_id = root_id 
            while fallback_id:
                # crawl down the chain
                debug('crawling root {}, on child {}'.format(root_id, fallback_id))

                self.pcur.execute("""
                SELECT fallback_campaign_id FROM campaign_properties WHERE campaign_id=%s
                """, (parent_id,))
                fallback_id = self.pcur.fetchone()['fallback_campaign_id']

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


    def queue_users(self):
        t = time()

        self.pcur.execute("""
            SELECT DISTINCT(visits.fbid) AS fbid FROM users 
                RIGHT JOIN visits 
                    ON visits.fbid=users.fbid 
                WHERE users.fbid IS NULL 
            UNION
            SELECT DISTINCT(edges.fbid_source) AS fbid FROM users
                RIGHT JOIN edges
                    ON users.fbid=edges.fbid_source
                WHERE users.fbid IS NULL
            """)

        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        info("Found {} unknown fbids".format(len(fbids)))

        self.fbids = self.fbids.union(set(fbids))

        info( 'Queued users for extraction in {}'.format(time()-t))


    def extract_user(self):
        """ Grab a fbid off of the queue and get it out of dynamo """
        if len(self.fbids) < 1: return
        fbid = self.fbids.pop()

        # null fbids :\
        if not fbid: return

        # USER DATA
        table = self.dconn.get_table('prod.users')
        data = defaultdict(lambda: None)
        try:
            debug('Seeking key {} in dynamo'.format(fbid))
            dyndata = table.get_item(fbid)

            # cast from epoch to dates and times
            if 'birthday' in dyndata and dyndata['birthday']:
                dyndata['birthday'] = datetime.date.fromtimestamp( int(dyndata['birthday']))

            if 'updated' in dyndata and dyndata['updated']:
                dyndata['updated'] = datetime.datetime.fromtimestamp( dyndata['updated'])

            data.update(dyndata)

        except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
            # this apparently is a real/possible thing, especially for legacy stuff
            # insert a blank row so we stop looking for it
            warning('fbid {} not found in dynamo!'.format(fbid))

            return

        # insert what we got
        self.pcur.execute("""
            INSERT INTO users
            (fbid, fname, lname, email, gender, birthday, city, state, updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (fbid, data['fname'], data['lname'], data['email'], 
                    data['gender'], data['birthday'], data['city'], data['state'], 
                    data['updated'])
            )

        self.pconn.commit()
        info( 'Successfully updated users table for fbid {}'.format(fbid))


    def queue_edges(self):
        self.pcur.execute("""
            SELECT DISTINCT fbid FROM visits 
            WHERE fbid NOT IN (SELECT DISTINCT fbid_target FROM edges)
            ORDER BY updated DESC
            """)

        fbids = [row['fbid'] for row in self.pcur.fetchall()]
        self.edge_fbids = self.edge_fbids.union( set(fbids))

        info("{} fbids queued for edge extraction".format(len(self.edge_fbids)))


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

            edges =[ ("({},{},{},{},{},{},{},{},{},{},{},{},'{}')".format(
                            edge['fbid_source'], edge['fbid_target'], edge['wall_comms'], edge['post_comms'], 
                            edge['tags'], edge['wall_posts'], edge['mut_friends'], edge['stat_likes'], 
                            edge['photos_other'], edge['post_likes'], edge['photos_target'], edge['stat_comms'], 
                            datetime.datetime.fromtimestamp( edge['updated'])) )
                    for edge in result.response['Items']]

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
            self.pcur.execute("INSERT INTO missingedges (fbid) VALUES (%s)", fbid)
            self.pconn.commit()
            return 

        info( 'Successfully updated edges table for fbid {}'.format(fbid))
        self.pconn.commit()



class App(ETL, tornado.web.Application):

    def __init__(self, debug, daemon=True):
        """
        Settings
        """
        settings = dict(
            cookie_secret="changemeplz",
            login_url="/login/", 
            template_path= "templates",
            static_path= "static",
            xsrf_cookies= False,
            debug = debug, #autoreloads on changes, among other things
        )

        """
        map URLs to Handlers, with regex patterns
        """
        handlers = [
        #    (r"/", MainHandler),
        ]

        # build connections to redshift, RDS
        self.connect()

        if daemon:
            # TODO: maintain connection, rebuild cursors
            P = tornado.ioloop.PeriodicCallback(self.connect, 600000)
            P.start()
   
            # keep our stats realtime
            self.extract()
            P = tornado.ioloop.PeriodicCallback(self.extract, 1000 * 60 * 10)
            P.start()

            # crawl for users and edges, lightly
            P = tornado.ioloop.PeriodicCallback(self.extract_user, 2000)
            P.start()
            P = tornado.ioloop.PeriodicCallback(self.extract_edge, 2000)
            P.start()

        tornado.web.Application.__init__(self, handlers, **settings)

def main():
    from tornado.options import define, options
    define("port", default=8001, help="run on the given port", type=int)
    define("debug", default=False, help="debug mode", type=bool)
    define("mkCSV", default=False, help="generate and upload client CSV file")

    tornado.options.parse_command_line()

    if options.mkCSV:
        # running as a batch job, don't daemonize
        from tasks import mkCSV
        app = App(options.debug, False)
        mkCSV(app)

    else:
        app = App(options.debug, True)
        http_server = tornado.httpserver.HTTPServer(app)
        http_server.listen(options.port)
        info( 'Serving on port %d' % options.port )
        tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

