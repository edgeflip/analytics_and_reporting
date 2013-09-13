from logging import debug, info, warning
import json
import MySQLdb

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import psycopg2
import psycopg2.extras
from tornado.web import HTTPError

from auth import Login, Logout, AuthMixin

class App(tornado.web.Application):

    def __init__(self):
        """
        Settings
        """
        settings = dict(
            cookie_secret="changemeplz",
            login_url="/login/", 
            template_path= "templates",
            static_path= "static",
            xsrf_cookies= False,
            debug = False, #autoreloads on changes, among other things
        )

        """
        map URLs to Handlers, with regex patterns
        """
        handlers = [
            (r"/", MainHandler),
            (r"/chartdata/", DataHandler),
            (r"/login/", Login),
            (r"/logout/", Logout),
        ]

        # build connections to redshift, RDS
        self.connect()

        #maintain connections, TCP keepalive doesn't seem to be working for redshift
        P = tornado.ioloop.PeriodicCallback(self.connect, 600000)
        P.start()

        #keep our stats realtime
        self.mkstats()
        P = tornado.ioloop.PeriodicCallback(self.mkstats, 120000)
        P.start()

        tornado.web.Application.__init__(self, handlers, **settings)

    def connect(self):
        """make db connections, would be cool to time this out"""

        debug('Connecting to redshift..')
        self.pconn = psycopg2.connect(host='wes-rs-inst.cd5t1q8wfrkk.us-east-1.redshift.amazonaws.com',
            user='edgeflip', database='edgeflip', port=5439, password='XzriGDp2FfVy9K')
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 

        # OperationalError: SSL SYSCALL error: EOF detected

        """
        debug('Connecting to RDS..')
        self.mconn = MySQLdb.connect(host='edgeflip-production-a-read1.cwvoczji8mgi.us-east-1.rds.amazonaws.com',
            user='root', passwd='YUUB2ctgkn8zfe', db='edgeflip')
        """
        debug('Done.')

    def mkstats(self):
        debug('Building client stats..')
        self.pcur.execute("DROP TABLE clientstats")

        megaquery = """

    CREATE TABLE clientstats AS
    SELECT
        e4.campaign_id,
        date_trunc('hour', t.updated) as time,
        SUM(CASE WHEN t.type='button_load' THEN 1 ELSE 0 END) AS visits,
        SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN t.type='authorized' THEN 1 ELSE 0 END) AS auths,
        COUNT(DISTINCT CASE WHEN t.type='authorized' THEN t.fbid ELSE NULL END) AS uniq_auths,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN t.fbid ELSE NULL END) AS shown,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.fbid ELSE NULL END) AS shares,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) AS audience,
        COUNT(DISTINCT CASE WHEN t.type='clickback' THEN t.cb_session_id ELSE NULL END) AS clickbacks

    FROM
        (
            SELECT e1.session_id, e1.campaign_id, e1.content_id, e1.ip, e1.fbid, e1.friend_fbid,
                e1.type, e1.appid, e1.content, e1.activity_id, NULL AS cb_session_id, e1.updated
            FROM events e1
                WHERE type <> 'clickback'
            UNION
            SELECT e3.session_id,
                e3.campaign_id,
                e2.content_id,
                e2.ip,
                e3.fbid,
                e3.friend_fbid,
                e2.type,
                e2.appid,
                e2.content,
                e2.activity_id,
                e2.session_id AS cb_session_id,
                e2.updated
            FROM events e2 LEFT JOIN events e3 USING (activity_id)
            WHERE e2.type='clickback' AND e3.type='shared'
        ) t
    LEFT JOIN (SELECT session_id,campaign_id FROM events WHERE type='button_load') e4
        USING (session_id)
    GROUP BY e4.campaign_id, date_trunc('hour', t.updated);
        """

        self.pcur.execute(megaquery)
        self.pconn.commit()

        debug('Done.')


class MainHandler(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):

        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
        }

        # look up campaigns
        q = """
            SELECT campaign_id, name FROM campaigns WHERE client_id=2 AND campaign_id IN
                (SELECT DISTINCT(campaign_id) FROM events WHERE type='button_load')
            ORDER BY campaign_id DESC
            """
        self.application.pcur.execute(q)
        ctx['campaigns'] = self.application.pcur.fetchall()

        return self.render('dashboard.html', **ctx)


class DataHandler(AuthMixin, tornado.web.RequestHandler):

    def post(self): 
        # grab args and pass them into the django view
        camp_id = self.get_argument('campaign')

        # magic case for the "aggregate" view
        if camp_id == 'aggregate':
            return self.finish(self.aggregate()) 

        camp_id = int(self.request.arguments['campaign'][0])
        day = self.request.arguments['day'][0]

        from views import chartdata
        data = chartdata(camp_id, day)

        self.finish(data)


    def aggregate(self):

        q = """
        SELECT meta.campaign_id, meta.name, visits, clicks, auths, uniq_auths,
                    shown, shares, audience, clickbacks
        FROM
            (SELECT campaign_id, SUM(visits) AS visits, SUM(clicks) AS clicks, SUM(auths) AS auths,
                    SUM(uniq_auths) AS uniq_auths, SUM(shown) AS shown, SUM(shares) AS shares,
                    SUM(audience) AS audience, SUM(clickbacks) AS clickbacks
                FROM clientstats
                GROUP BY campaign_id
            ) AS stats,

            (SELECT campaign_id, name FROM campaigns WHERE client_id=2) AS meta

        WHERE stats.campaign_id=meta.campaign_id
        ORDER BY meta.campaign_id DESC;
        """

        self.application.pcur.execute(q)

        # GOOGlify it
        aggdata = []
        for row in self.application.pcur.fetchall():
            aggdata.append( {'c': [{'v':x} for x in row[1:]]})
   
        from views import MONTHLY_METRICS 
        metrics = MONTHLY_METRICS[:]
        metrics[0] = {'type':'string', 'id':'campname', 'label':'Campaign Name'}
    
        out = {'cols': metrics, 'rows': aggdata}

        return out


def main():
    from tornado.options import define, options
    define("port", default=8001, help="run on the given port", type=int)
    define("runtests", default=False, help="run tests", type=bool)

    tornado.options.parse_command_line()

    if options.runtests:
        #put tests in the tests folder
        import tests, unittest
        import sys
        sys.argv = ['main.py',] #unittest goes digging in argv
        unittest.main( 'tests')
        return

    http_server = tornado.httpserver.HTTPServer( App() )
    http_server.listen(options.port)
    info( 'Serving on port %d' % options.port )
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

