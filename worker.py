from logging import debug, info, warning

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from tasks import ETL, mail_tracebacks


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
 
    @mail_tracebacks
    def fromRDS(self): 
        """
        Pretty basic ETL sort of loop for grabbing tables out of RDS
        """
        self.extract()
        P = tornado.ioloop.PeriodicCallback(self.extract, 1000 * 60 * 10)
        P.start()

    @mail_tracebacks
    def fromDynamo(self):
        """
        Crawl for fbids from RDS records and spider out into dynamo.. because
        we need to stream records out at a steady pace, we queue things up
        according to various priorities
        """

        self.queue_edges()
        self.queue_users()
        P = tornado.ioloop.PeriodicCallback(self.queue_users, 1000 * 60 * 15)
        P.start()
        P = tornado.ioloop.PeriodicCallback(self.queue_edges, 1000 * 60 * 15)
        P.start()

        # top priority, grab new users
        P = tornado.ioloop.PeriodicCallback(self.extract_user, 1000 * .8)
        P.start()
        # and edge info
        P = tornado.ioloop.PeriodicCallback(self.extract_edge, 2000)
        P.start()
        # slightly less priority, updating users
        P = tornado.ioloop.PeriodicCallback(self.refresh_user, 1000 * 5)
        P.start()


def main():
    from tornado.options import define, options
    define("debug", default=False, help="debug mode", type=bool)
    define("fromRDS", default=False, help="ETL process for data from RDS")
    define("fromDynamo", default=False, help="ETL process for data from Dynamo")
    define("mkCSV", default=False, help="generate and upload client CSV file")
    define("mkVAEmail", default=False, help="generate and email VA a CSV of stats with sources")

    tornado.options.parse_command_line()

    if options.mkCSV:
        from tasks import mkCSV, mkemailCSV
        app = App(options.debug)
        mkCSV(app)
        mkemailCSV(app)

    elif options.mkVAEmail:
        from tasks import mkSumEmail
        app = App(options.debug)
        mkSumEmail(app)

    else:
        app = App(options.debug)

        if options.fromDynamo:
            app.fromDynamo()

        if options.fromRDS:
            app.fromRDS()

        tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()

