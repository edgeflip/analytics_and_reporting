from logging import debug, info, warning
import json
import MySQLdb
from time import strftime
from collections import defaultdict

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
            (r"/", MainHandler),  # main client template
            (r"/tabledata/?", ClientSummary),  # client summary data
            (r"/dailydata/", DailyData),
            (r"/hourlydata/", HourlyData),
            (r"/alldata/", AllData),
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

        debug('Done.')

    def update(self):
        """ Someday, sync this with the data moving, somehow """
        self.updated = strftime('%x %X')


class MainHandler(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):

        """Basically just loads template.. why does this query for campaigns?"""

        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'updated': self.application.updated,
        }

        return self.render('clientsum.html', **ctx)


class ClientSummary(AuthMixin, tornado.web.RequestHandler):
    @tornado.web.authenticated
    def get(self, client=2):  # at some point, grab client by looking at the auth'd user

        # very similar to the sums per campaign, but join on root campaign
        self.application.pcur.execute("""
            SELECT meta.root_id, meta.name, visits, clicks, auths, uniq_auths,
                        shown, shares, audience, clickbacks
            FROM
                (SELECT campchain.root_id, SUM(visits) AS visits, SUM(clicks) AS clicks, SUM(auths) AS auths,
                        SUM(uniq_auths) AS uniq_auths, SUM(shown) AS shown, SUM(shares) AS shares,
                        SUM(audience) AS audience, SUM(clickbacks) AS clickbacks
                    FROM clientstats, campchain
                    WHERE campchain.parent_id=clientstats.campaign_id
                    GROUP BY root_id
                ) AS stats,

                (SELECT campchain.root_id, campaigns.campaign_id, campaigns.name 
                    FROM campaigns, campchain
                    WHERE campchain.parent_id=campaigns.campaign_id
                    AND client_id=2
                ) AS meta

            WHERE stats.root_id=meta.campaign_id
            ORDER BY meta.root_id DESC;
        """, (client,))

        return self.finish(json.dumps([dict(row) for row in self.application.pcur.fetchall()]))


import datetime
class AllData(AuthMixin, tornado.web.RequestHandler):
    @tornado.web.authenticated
    def post(self): 
        camp_id = self.get_argument('campaign')

        # first, grab data for the bigger chart, grouped and summed by day
        self.application.pcur.execute("""
        SELECT DATE_TRUNC('hour', hour) as time,
            SUM(visits) AS visits,
            SUM(clicks) AS clicks,
            SUM(auths) AS auths,
            SUM(uniq_auths) AS uniq_auths,
            SUM(shown) AS shown,
            SUM(shares) AS shares,
            SUM(audience) AS audience,
            SUM(clickbacks) AS clickbacks
            
        FROM clientstats,campchain 
        WHERE clientstats.campaign_id=campchain.parent_id
        AND campchain.root_id=%s
        GROUP BY time
        ORDER BY time ASC
        """, (camp_id,))

        def mangle(row):
            row = dict(row)
            row['time'] = row['time'].isoformat()
            return row

        data = [mangle(row) for row in self.application.pcur.fetchall()]

        # day = self.get_argument('day', None)
        self.finish({'data':data})


class HourlyData(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def post(self): 

        # grab args and pass them into the django view
        camp_id = self.get_argument('campaign')
        day = self.get_argument('reqdate')
        day = datetime.datetime.strptime(day, "%Y-%m-%dT%H:%M:%S.%fZ")

        info( 'Getting hourly data for campaign {}, day {}'.format(camp_id, day))


        """
        super awks, naming the field `day` even though it's an hour, to match the daily format
        to make the front end code more "usable"
        """

        # first, grab data for the bigger chart, grouped and summed by day
        self.application.pcur.execute("""
        SELECT DATE_TRUNC('hour', hour) as time,
            SUM(visits) AS visits,
            SUM(clicks) AS clicks,
            SUM(auths) AS auths,
            SUM(uniq_auths) AS uniq_auths,
            SUM(shown) AS shown,
            SUM(shares) AS shares,
            SUM(audience) AS audience,
            SUM(clickbacks) AS clickbacks
            
        FROM clientstats,campchain 
        WHERE clientstats.campaign_id=campchain.parent_id
        AND campchain.root_id=%s
        AND DATE_TRUNC('day', hour) = %s
        GROUP BY time
        ORDER BY time ASC
        """, (camp_id,day))

        def mangle(row):
            row = dict(row)
            row['time'] = row['time'].isoformat()
            return row

        data = [mangle(row) for row in self.application.pcur.fetchall()]

        # day = self.get_argument('day', None)
        self.finish({'data':data})


class DailyData(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def post(self): 
        # grab args and pass them into the django view
        camp_id = self.get_argument('campaign')
        info( 'Getting daily data for campaign {}'.format(camp_id))

        # first, grab data for the bigger chart, grouped and summed by day
        self.application.pcur.execute("""
        SELECT DATE_TRUNC('day', hour) as time,
            SUM(visits) AS visits,
            SUM(clicks) AS clicks,
            SUM(auths) AS auths,
            SUM(uniq_auths) AS uniq_auths,
            SUM(shown) AS shown,
            SUM(shares) AS shares,
            SUM(audience) AS audience,
            SUM(clickbacks) AS clickbacks
            
        FROM clientstats,campchain 
        WHERE clientstats.campaign_id=campchain.parent_id
        AND campchain.root_id=%s
        GROUP BY time 
        ORDER BY time ASC
        """, (camp_id,))

        def mangle(row):
            row = dict(row)
            row['time'] = row['time'].isoformat()
            return row

        data = [mangle(row) for row in self.application.pcur.fetchall()]

        # day = self.get_argument('day', None)
        self.finish({'data':data})


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

