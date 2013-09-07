from logging import debug, info, warning
import json

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import psycopg2
import psycopg2.extras
from tornado.web import HTTPError

from auth import Login, Logout

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
            debug = True, #autoreloads on changes, among other things
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

        # build connections to redshift
        self.connect()

        tornado.web.Application.__init__(self, handlers, **settings)

    def connect(self):
        self.pconn = psycopg2.connect(host='wes-rs-inst.cd5t1q8wfrkk.us-east-1.redshift.amazonaws.com',
            user='edgeflip', database='edgeflip', port=5439, password='XzriGDp2FfVy9K')
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 


class MainHandler(tornado.web.RequestHandler):
    def get(self):

        ctx = {
            'STATIC_URL':'/static/',
            'user':'nobody',
        }

        # look up campaigns in our ghetto
        q = "SELECT campaign_id, name FROM campaigns WHERE client_id=2 AND NOT is_deleted"
        self.application.pcur.execute(q)
        ctx['campaigns'] = self.application.pcur.fetchall()

        return self.render('dashboard.html', **ctx)

class DataHandler(tornado.web.RequestHandler):
    def post(self): 
        # grab args and pass them into the django view
        debug(self.request.arguments)
        camp_id = int(self.request.arguments['campaign'][0])
        day = self.request.arguments['day'][0]

        from views import chartdata
        data = chartdata(camp_id, day)

        self.finish(data)


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

