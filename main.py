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
            (r"/", MainHandler),
            (r"/chartdata/", DataHandler),
            (r"/login/", Login),
            (r"/logout/", Logout),
        ]

        # build connections to redshift, RDS
        self.connect()

        # bandaid to maintain connections
        P = tornado.ioloop.PeriodicCallback(self.connect, 600000)
        P.start()

        # lol
        self.update()
        P = tornado.ioloop.PeriodicCallback(self.update, 150000)

        tornado.web.Application.__init__(self, handlers, **settings)

    def connect(self):
        """make db connections, would be cool to time this out"""

        debug('Connecting to redshift..')
        from keys import redshift
        self.pconn = psycopg2.connect( **redshift)

        # TODO flip autocommit on in this cursor, dodge hanging transactions
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 

        """
        debug('Connecting to RDS..')
        self.mconn = MySQLdb.connect(host='edgeflip-production-a-read1.cwvoczji8mgi.us-east-1.rds.amazonaws.com',
            user='root', passwd='YUUB2ctgkn8zfe', db='edgeflip')
        """
        debug('Done.')

    def update(self):
        """ Someday, sync this with the data moving, somehow """
        self.updated = strftime('%x %X')


class MainHandler(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):

        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'updated': self.application.updated,
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
        data = chartdata(camp_id, self.application.pcur, day)

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
    define("debug", default=False, help="debug mode", type=bool)

    tornado.options.parse_command_line()

    http_server = tornado.httpserver.HTTPServer( App(options.debug) )
    http_server.listen(options.port)
    info( 'Serving on port %d' % options.port )
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

