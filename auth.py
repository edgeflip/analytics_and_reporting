import tornado.web
import base64
import json
import functools
from hashlib import sha256

from django.utils.crypto import pbkdf2
# from django.contrib.auth import PBKDF2PasswordHasher

class Logout(tornado.web.RequestHandler):
    def get(self, *args):
        self.clear_cookie("user")
        self.redirect('/login/')


class Login(tornado.web.RequestHandler):
    def get(self, *args):
        self.render("login.html", error=None) 

    def post(self):
        login = self.get_argument('login')  # this will error if there's no login arg, iirc
        password = self.get_argument('password')

        #legacy magicks
        if (login == 'virginia') and (password=='sharing'):
            self.set_secure_cookie('user', login)  
            self.set_secure_cookie('superuser', 'false')  
            return self.redirect('/')

        # check that this user exists, get password string to compare
        exists = self.application.mcur.execute("""
            SELECT password, is_superuser FROM auth_user WHERE username=%s
            """, (login,))

        if exists != 1:
            return self.render("login.html", error='Wrong username.')

        # unpack django structures
        bighash, superuser = self.application.mcur.fetchone()
        algo, iterations, salt, passhash = bighash.split('$')
        superuser = 'true' if superuser == 1 else 'false'  # json for the cookie

        # check password:
        if self.encode(password,salt,int(iterations)) != unicode(bighash):
            return self.render("login.html", error='Wrong password.')

        # looks good
        self.set_secure_cookie('user', login)  
        self.set_secure_cookie('superuser', superuser)  
        return self.redirect('/')  # should actually redirect to self.get_argument('next')

    # swiped mostly from django.contrib.auth.hashers.PBKDF2PasswordHasher
    def encode(self, password, salt, iterations=None):
        assert password is not None
        assert salt and '$' not in salt
        if not iterations:
            iterations = self.iterations
        hash = pbkdf2(password, salt, iterations, digest=sha256)
        hash = base64.b64encode(hash).decode('ascii').strip()
        return "%s$%d$%s$%s" % ("pbkdf2_sha256", iterations, salt, hash)


class AuthMixin(object):
    def get_current_user(self):
        return self.get_secure_cookie('user')

    @property
    def superuser(self):
        return json.loads( self.get_secure_cookie('superuser'))


def authorized(method):
    """
    Decorate methods with this, underneath @tornado.web.authenticated, to require
    a user be logged in and have superuser status.

    Underprivileged Users get sent to the login url with an error message telling them
    to login as someone else
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.superuser:
            return self.render("login.html", error='You are not authorized to view that page.')

        return method(self, *args, **kwargs)
    return wrapper
