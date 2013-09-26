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
            (r"/", MainHandler),
            (r"/summary/?", Summary),
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
                (SELECT DISTINCT(root_id) FROM campchain)
            ORDER BY campaign_id DESC;
            """
        self.application.pcur.execute(q)
        ctx['campaigns'] = self.application.pcur.fetchall()

        return self.render('dashboard.html', **ctx)


class Summary(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):

        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'updated': self.application.updated,
            'defaultdict': defaultdict,
        }

        ctx.update( self.aggregate())

        return self.render('sumtable.html', **ctx)

    def aggregate(self, client=2):

        # get all the summary data
        self.application.pcur.execute("""
            SELECT meta.campaign_id, meta.name, visits, clicks, auths, uniq_auths,
                        shown, shares, audience, clickbacks
            FROM
                (SELECT campaign_id, SUM(visits) AS visits, SUM(clicks) AS clicks, SUM(auths) AS auths,
                        SUM(uniq_auths) AS uniq_auths, SUM(shown) AS shown, SUM(shares) AS shares,
                        SUM(audience) AS audience, SUM(clickbacks) AS clickbacks
                    FROM clientstats
                    GROUP BY campaign_id
                ) AS stats,
    
                (SELECT campaign_id, name FROM campaigns WHERE client_id=%s) AS meta
    
            WHERE stats.campaign_id=meta.campaign_id
            ORDER BY meta.campaign_id DESC;
        """, (client,))

        # index by campaign_id
        aggdata = {row['campaign_id']:dict(row) for row in self.application.pcur.fetchall()}

        # look up root campaigns
        self.application.pcur.execute("""
            SELECT root_id,parent_id
            FROM campchain 
    
            LEFT JOIN 
                (SELECT DISTINCT(campaign_id) FROM campaigns WHERE client_id=%s) as clientcampaigns
            ON clientcampaigns.campaign_id=campchain.parent_id
    
            WHERE parent_id IS NOT NULL
            ORDER BY root_id DESC, parent_id DESC;
        """, (client,))

        # build chains as lists
        chains = defaultdict(lambda: [])
        for row in self.application.pcur.fetchall():
            chains[row['root_id']].append( row['parent_id'])

        # but we need them ordered.. so this is probably silly
        chainkeys = chains.keys()
        chainkeys.sort()
        chainkeys.reverse()

        # build summary data per root
        sumdata = {}
        for root in chains:
            d = defaultdict(lambda:[])
            for child in chains[root]:
                if child not in aggdata: continue
                _data = aggdata[child]
                [d[k].append(_data[k]) for k in _data]
            del d['name']

            sumdata[root] = {k:sum(d[k]) for k in d}

        # grab campaign metadata
        self.application.pcur.execute("""
        SELECT campaign_id, name FROM campaigns WHERE client_id=%s
        """, (client,))
        campmeta = {row['campaign_id']:dict(row) for row in self.application.pcur.fetchall()}

        """
        # GOOGlify it
        aggdata = []
        for row in self.application.pcur.fetchall():
            aggdata.append( {'c': [{'v':x} for x in row[1:]]})
        """
   
        from views import MONTHLY_METRICS 
        metrics = MONTHLY_METRICS[:]
        metrics[0] = {'type':'string', 'id':'campname', 'label':'Campaign Name'}

        return {'data':aggdata, 'sumdata':sumdata, 'chains':chains, 'chainkeys':chainkeys, 'cols':metrics, 'meta':campmeta}


class DataHandler(AuthMixin, tornado.web.RequestHandler):

    def post(self): 
        # grab args and pass them into the django view
        camp_id = self.get_argument('campaign')
        info( 'Getting data for campaign {}'.format(camp_id))

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

