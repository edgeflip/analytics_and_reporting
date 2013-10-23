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
            (r"/edgeplorer/?", Edgeplorer),  # internal fbid explorer template
            (r"/tabledata/", ClientSummary),  # client summary data
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

        debug('Connecting to RDS..')
        from keys import rds
        self.mconn = MySQLdb.connect( **rds)

        # TODO flip autocommit on in this cursor, dodge hanging transactions
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 

        debug('Done.')

    def update(self):
        """ Someday, sync this with the data moving, somehow """
        self.updated = strftime('%x %X')


class Edgeplorer(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):
        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'updated': self.application.updated,
        }

        return self.render('edgeplorer.html', **ctx)

    def post(self):
        fbid = int(self.get_argument('fbid')) 

        #datetimes aren't JSON serializable :(
        def mangle(row, keys=['time',]):
            row = dict(row)
            for key in keys:
                row[key] = row[key].isoformat() if row[key] else None
            return row

        self.application.pcur.execute("""
        SELECT * FROM users WHERE fbid=%s
        """, (fbid,))
        users = [mangle(row, ['birthday','updated']) for row in self.application.pcur.fetchall()]

        self.application.pcur.execute("""
        SELECT events.* FROM events,visits 
        WHERE events.visit_id=visits.visit_id 
            AND fbid=%s 
        ORDER BY event_datetime ASC;
        """, (fbid,))
        events = [mangle(row, ['updated', 'event_datetime', 'created']) for row in self.application.pcur.fetchall()]

        self.finish( {'users':users, 'events':events})



class MainHandler(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):

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

