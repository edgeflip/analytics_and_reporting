from logging import debug, info, warning
import json
import MySQLdb
from time import strftime

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import psycopg2
import psycopg2.extras
from tornado.web import HTTPError

from auth import Login, Logout, AuthMixin

class App(tornado.web.Application):

    def __init__(self, debug):
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

        # TODO: maintain connection, rebuild cursors
        P = tornado.ioloop.PeriodicCallback(self.connect, 600000)
        P.start()

        #keep our stats realtime
        self.mkstats()
        P = tornado.ioloop.PeriodicCallback(self.mkstats, 120000)
        P.start()

        tornado.web.Application.__init__(self, handlers, **settings)

    def connect(self):
        """make db connections, would be cool to time this out"""

        """
        I think that long running transactions cause:
        # OperationalError: SSL SYSCALL error: EOF detected
        """
        debug('Connecting to redshift..')
        from keys import redshift
        self.pconn = psycopg2.connect( **redshift)
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 

        """
        debug('Connecting to RDS..')
        from keys import rds
        self.mconn = MySQLdb.connect( **rds)
        """
        debug('Done.')

    def mkstats(self):

        # get tables out of RDS and into redshift
        from table_to_redshift import main as rds2rs
        debug('Uploading events..')
        rds2rs('events', self.pcur)

        debug('Uploading visits..')
        rds2rs('events', self.pcur)

        debug('Uploading campaigns..')
        rds2rs('campaigns', self.pcur)

        # create the dashboard stats table.
        debug('Building client stats..')
        self.pcur.execute("DROP TABLE clientstats")

        megaquery = """

    CREATE TABLE clientstats AS
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
        ) t


    LEFT JOIN (SELECT visit_id,campaign_id FROM events WHERE type='button_load') e4
        USING (visit_id)
    INNER JOIN (SELECT fbid, visit_id FROM visits) v
        USING (visit_id)
    GROUP BY t.campaign_id, hour
        """

        self.pcur.execute(megaquery)
        self.pconn.commit()

        self.updated = strftime('%x %X')

        debug('Done.')


def main():
    from tornado.options import define, options
    define("port", default=8001, help="run on the given port", type=int)
    define("debug", default=False, help="debug mode", type=bool)

    tornado.options.parse_command_line()

    http_server = tornado.httpserver.HTTPServer( App(options.debug) )
    http_server.listen(options.port)
    info( 'Serving on port %d' % options.port )
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

