from logging import debug, info, warning

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tasks import ETL


class App(ETL, tornado.web.Application):

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

        # If we end up running this as a daemon, refresh the connections and cursors every so often
        P = tornado.ioloop.PeriodicCallback(self.connect, 600000)
        P.start()

        tornado.web.Application.__init__(self, handlers, **settings)
 
 
    def fromRDS(self): 
        # keep our stats realtime
        self.extract()
        P = tornado.ioloop.PeriodicCallback(self.extract, 1000 * 60 * 10)
        P.start()


    def fromDynamo(self):
        self.queue_edges()
        self.queue_users()
        P = tornado.ioloop.PeriodicCallback(self.queue_users, 1000 * 60 * 15)
        P.start()
        P = tornado.ioloop.PeriodicCallback(self.queue_edges, 1000 * 60 * 15)
        P.start()

        # top priority, grab new users
        P = tornado.ioloop.PeriodicCallback(self.extract_user, 1000 * .8)
        P.start()
        # slightly less priority, updating users
        P = tornado.ioloop.PeriodicCallback(self.refresh_user, 1000 * 5)
        P.start()
        P = tornado.ioloop.PeriodicCallback(self.extract_edge, 2000)
        P.start()


def main():
    from tornado.options import define, options
    define("port", default=8001, help="run on the given port", type=int)
    define("debug", default=False, help="debug mode", type=bool)
    define("fromRDS", default=False, help="ETL process for data from RDS")
    define("fromDynamo", default=False, help="ETL process for data from Dynamo")
    define("mkCSV", default=False, help="generate and upload client CSV file")

    tornado.options.parse_command_line()

    if options.mkCSV:
        # running as a batch job, don't daemonize
        from tasks import mkCSV, mkemailCSV
        app = App(options.debug, False)
        mkCSV(app)
        mkemailCSV(app)

    else:
        app = App(options.debug)

        if options.fromDynamo:
            app.fromDynamo()

        if options.fromRDS:
            app.fromRDS()

        http_server = tornado.httpserver.HTTPServer(app)
        http_server.listen(options.port)
        info( 'Serving on port %d' % options.port )
        tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

