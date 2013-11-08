import functools
import smtplib
import email
import traceback
from hashlib import md5
from logging import debug, info, warning

import tornado.web

"""
Uncaught exceptions are sent via email to the list of RECIPIENTS below, 
with a hash of the traceback to help gmail keep threads straight.

IMPORTANT: debug mode (--debug=True from the command line) turns this off!
"""



RECIPIENTS = ['japhy@edgeflip.com',]


def mail_tracebacks(method):
    """
    Decorate arbitrary methods to forward tracebacks on to concerned parties
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        #check for debug mode
        try:
            return method(self, *args, **kwargs)
        except:
            # if debug mode is off, email the stack trace
            from tornado.options import options
            if options.debug: raise
            else: traceback.print_exc()


            err = traceback.format_exc()

            msg = email.Message.Message()
            msg['Subject'] = 'UNHANDLED EXCEPTION {}'.format(md5(err).hexdigest())
            msg.set_payload(err)

            smtp = smtplib.SMTP()
            smtp.connect()
            smtp.sendmail('syncerror@edgeflip.com', RECIPIENTS, msg.as_string()) 
    return wrapper


class ErrorHandler(object):
    """
    Mixin to tornado.request.handler to email off tracebacks for uncaught exceptions
    """

    def send_error(self, status_code=500, **kwargs):
        from tornado.options import options
        if not options.debug:

            err = traceback.format_exc()

            payload = """
REQUEST
{}

TRACEBACK
{}
            """.format( self.request, err)
            msg = email.Message.Message()
            msg['Subject'] = 'UNHANDLED EXCEPTION {}'.format(md5(err).hexdigest())
            msg.set_payload(payload)
            
            smtp = smtplib.SMTP()
            smtp.connect()
            smtp.sendmail('dashboarderror@edgeflip.com', RECIPIENTS, msg.as_string())

        super(ErrorHandler, self).send_error()


class Errorer(ErrorHandler, tornado.web.RequestHandler):

    def get(self):
        raise Exception

    def post(self):
        raise Exception
