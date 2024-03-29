from logging import debug, info, warning
import json
import MySQLdb
from time import strftime

from datetime import datetime, timedelta

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
        from errors import Errorer
        handlers = [
            (r"/", MainHandler),  # main client template
            (r"/edgeplorer/?", Edgeplorer),  # internal fbid explorer template
            (r"/edgedash/?", Edgedash),  # internal dashboard
            (r"/tabledata/", ClientSummary),  # client summary data for all campaigns
            (r"/alldata/", AllData),  # hourly data for a particular campaign
            (r"/login/", Login),
            (r"/logout/", Logout),
            (r"/chicken_little/", Errorer),
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
        """set up database connections"""
        debug('Connecting to redshift..')
        from keys import redshift
        self.pconn = psycopg2.connect( **redshift)
        # TODO flip autocommit on in this cursor, dodge hanging transactions
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 

        debug('Connecting to RDS..')
        from keys import rds
        self.mconn = MySQLdb.connect( **rds)
        self.mcur = self.mconn.cursor()
        
        debug('Done.')


    def update(self):
        """ 
        There is a line in the dashboard template that shows 'Data last updated
        at X'.  Someday, somehow sync that up with the worker processes, but for
        now...
        """
        self.updated = strftime('%x %X')



class MainHandler(AuthMixin, tornado.web.RequestHandler):

    @tornado.web.authenticated
    def get(self):

        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'superuser': self.superuser,
            'updated': self.application.updated,
        }

        # if it's a superuser, look up the clients to populate the chooser
        if self.superuser:
            self.application.mcur.execute("""
                SELECT client_id, name FROM clients ORDER BY client_id DESC
            """)
            clients = [row for row in self.application.mcur.fetchall()]
            ctx['clients'] = clients

        return self.render('clientdash.html', **ctx)



class ClientSummary(AuthMixin, tornado.web.RequestHandler):
    """
    Data for the initial pageview, a summary of client stats grouped by campaign
    """

    @tornado.web.authenticated
    def get(self):
        client = int(self.get_argument('client'))  # let junk 500

        if not client:
            # browser sent 0, look up by cookie
            client = self.client
        else:
            # check authorization for arbitrary client ids
            if not self.superuser: raise HTTPError(403)

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
                    AND client_id=%s
                ) AS meta

            WHERE stats.root_id=meta.campaign_id
            ORDER BY meta.root_id DESC;
        """, (client,))

        return self.finish(json.dumps([dict(row) for row in self.application.pcur.fetchall()]))


class AllData(AuthMixin, tornado.web.RequestHandler):
    """ Data for a particular campaign, used by the chart view """

    @tornado.web.authenticated
    def post(self): 
        camp_id = self.get_argument('campaign')

        #minor TODO: check that the signed in user is authorized to pull data for this campaign

        # grab data for the chart, grouped and summed by day
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
        self.finish({'data':data})


from auth import authorized
class Edgeplorer(AuthMixin, tornado.web.RequestHandler):
    """
    Internal tool that takes a fbid and looks up relevant user/visit/event records
    """

    @tornado.web.authenticated
    @authorized
    def get(self):
        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'updated': self.application.updated,
        }

        return self.render('edgeplorer.html', **ctx)

    def post(self):
        try:
            fbid = int(self.get_argument('fbid')) 
        except:
            raise HTTPError(404)

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
        debug(users)

        self.application.pcur.execute("""
        SELECT events.* FROM events,visits,visitors
        WHERE events.visit_id=visits.visit_id 
            AND visits.visitor_id=visitors.visitor_id
            AND fbid=%s 
        ORDER BY event_datetime ASC;
        """, (fbid,))
        events = [mangle(row, ['updated', 'event_datetime', 'created']) for row in self.application.pcur.fetchall()]
        debug(events)

        self.application.pcur.execute("""
        SELECT * FROM edges WHERE fbid_target=%s;
        """, (fbid,))
        edges = [mangle(row, ['updated',]) for row in self.application.pcur.fetchall()]
        debug(edges)

        self.finish( {'users':users, 'events':events, 'edges':edges})


class Edgedash(AuthMixin, tornado.web.RequestHandler):
    """ 
    A very half baked visualization of all events by type 
    see internaldash.js for the front end
    """

    @tornado.web.authenticated
    @authorized
    def get(self):
        # render a base template
        ctx = {
            'STATIC_URL':'/static/',
            'user': self.get_current_user(),
            'superuser': self.superuser,
            }

        return self.render('internaldash.html', **ctx)

    @tornado.web.authenticated
    @authorized
    def post(self):
        # load various data sets I suppose.

        # at some point let the UI configure the timespan but whatevs for right now
        tstart = self.get_argument('tstart', datetime.today() - timedelta(days=1))
        # tend = self.get_argument('tend', datetime.today())

        # hrm, kinda want to group by campaign_id also
        self.application.pcur.execute("""
        SELECT type, COUNT(event_id), DATE_TRUNC('hour', event_datetime) AS hour, events.campaign_id, campaigns.name
        FROM events, campaigns
        WHERE event_datetime > %s
        AND campaigns.campaign_id=events.campaign_id
        GROUP BY hour, type, events.campaign_id, campaigns.name
        """, (tstart,))

        def mangle(row):
            row = dict(row)
            row['hour'] = row['hour'].isoformat()
            return row

        data = [mangle(row) for row in self.application.pcur.fetchall()]

        return self.finish({'data':data})


def main():
    from tornado.options import define, options
    define("port", default=8001, help="run on the given port", type=int)
    define("debug", default=True, help="debug mode", type=bool)

    tornado.options.parse_command_line()

    # also send logs through syslog to get them into graylog
    if not options.debug:
        """
        This will send things through syslogd, and into /var/log/syslog by default.
        to get them into graylog, you probably need to tinker with rsyslog or syslog-ng
        settings. see: http://dev.nuclearrooster.com/2011/04/05/forwarding-rsyslog-to-graylog2/
        for simple examples
        """
        import logging
        import logging.handlers
        logger = logging.getLogger() 
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        handler.setFormatter(tornado.log.LogFormatter(color=False))
        logger.addHandler(handler)

    http_server = tornado.httpserver.HTTPServer( App(options.debug) )
    http_server.listen(options.port)
    info( 'Serving on port %d' % options.port )
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

