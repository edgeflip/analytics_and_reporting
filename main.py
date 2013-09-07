from logging import info
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import psycopg2
import psycopg2.extras
from tornado.web import HTTPError

class App( tornado.web.Application):
    def __init__(self):
        """
        Settings
        """
        settings = dict(
            cookie_secret="changemeplz",
            login_url="/login", 
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
        ]

        # build connections to redshift
        self.connect()

        tornado.web.Application.__init__(self, handlers, **settings)

    def connect(self):
        self.pconn = psycopg2.connect(host='wes-rs-inst.cd5t1q8wfrkk.us-east-1.redshift.amazonaws.com',
            user='edgeflip', database='edgeflip', port=5439, password='XzriGDp2FfVy9K')
        self.pcur = self.pconn.cursor(cursor_factory = psycopg2.extras.DictCursor) 


class MainHandler( tornado.web.RequestHandler):
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

