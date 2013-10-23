import tornado.web
import base64

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
        # check that this user exists, get password string to compare
        login = self.get_argument('login')  # this will error if there's no login arg, iirc

        password = self.get_argument('password')

        if (password != 'sharing'):
            return self.render("login.html", error='Wrong username or password.')

        self.set_secure_cookie('user', login)  

        self.redirect('/')

    # swiped mostly from django.contrib.auth.PBKDF2PasswordHasher
    def encode(self, password, salt, iterations=None):
        assert password is not None
        assert salt and '$' not in salt
        if not iterations:
            iterations = self.iterations
        hash = pbkdf2(password, salt, iterations, digest=self.digest)
        hash = base64.b64encode(hash).decode('ascii').strip()
        return "%s$%d$%s$%s" % (self.algorithm, iterations, salt, hash)


class AuthMixin(object):
    def get_current_user(self):
        return self.get_secure_cookie('user')
