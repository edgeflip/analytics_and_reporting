import tornado.web
import base64
import json
import functools
from hashlib import sha256
from logging import debug, info

# kind of silly, django is in requirements.txt just for this:
from django.utils.crypto import pbkdf2
# from django.contrib.auth import PBKDF2PasswordHasher  # fails because of missing settings.py

class Logout(tornado.web.RequestHandler):
    def get(self, *args):
        # clear whatever cookies have been set
        self.clear_cookie("user")
        self.clear_cookie("superuser")
        self.clear_cookie("client")

        # redirect to /login/, nothing else is visible to anonymous users
        self.redirect('/login/')


class Login(tornado.web.RequestHandler):
    def get(self, *args):
        self.render("login.html", error=None) 

    def post(self):
        """
        Take login/password args from the browser and validate them against
        Django tables in RDS
        """

        login = self.get_argument('login')  # this will error if there's no login arg, iirc
        password = self.get_argument('password')

        # check that this user exists, get password string to compare, and client_id
        exists = self.application.mcur.execute("""
            SELECT password, is_superuser, client_id
            FROM auth_user
                LEFT JOIN auth_user_groups ON auth_user_groups.user_id=auth_user.id
                LEFT JOIN clients_auth_groups ON auth_user_groups.group_id=clients_auth_groups.group_id
            WHERE username=%s
            """, (login,))

        debug(exists)
        if exists < 1:
            """ in the case of users who belong to multiple clients, this logic
            will break down - but so will the UX, so punt the whole issue
            """
            return self.render("login.html", error='Wrong username.')

        # unpack django structures
        bighash, superuser, client_id = self.application.mcur.fetchone()
        algo, iterations, salt, passhash = bighash.split('$')
        superuser = 'true' if superuser == 1 else 'false'  # json for the cookie

        # check password:
        if self.encode(password,salt,int(iterations)) != unicode(bighash):
            return self.render("login.html", error='Wrong password.')

        # looks good
        self.set_secure_cookie('user', login)  
        self.set_secure_cookie('superuser', superuser)  
        self.set_secure_cookie('client', json.dumps(client_id))  
        return self.redirect('/')  # should actually redirect to self.get_argument('next')

    # swiped almost exactly from django.contrib.auth.hashers.PBKDF2PasswordHasher
    def encode(self, password, salt, iterations=None):
        assert password is not None
        assert salt and '$' not in salt
        if not iterations:
            iterations = self.iterations
        hash = pbkdf2(password, salt, iterations, digest=sha256)
        hash = base64.b64encode(hash).decode('ascii').strip()
        return "%s$%d$%s$%s" % ("pbkdf2_sha256", iterations, salt, hash)


class AuthMixin(object):
    """ 
    Add this to RequestHandler objects that will require auth, and decorate 
    the appropriate methods wth @tornado.web.authenticated
    """

    def get_current_user(self):
        try:
            # make sure, if we add cookies, that malformed-but-still-logged-in sessions get bounced
            assert self.get_secure_cookie('user')
            assert self.get_secure_cookie('superuser')
            assert self.get_secure_cookie('client')
        except AssertionError:
            return None


        return self.get_secure_cookie('user')

    @property
    def superuser(self):
        return json.loads(self.get_secure_cookie('superuser'))

    @property
    def client(self):
        """
        Essentially "caching" the client_id for this user in a cookie to
        save a database hit on incoming requests
        """
        return json.loads(self.get_secure_cookie('client'))


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

