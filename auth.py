import tornado.web

class Logout(tornado.web.RequestHandler):
    def get(self, *args):
        self.clear_cookie("user")
        self.redirect('/login/')


class Login(tornado.web.RequestHandler):
    def get(self, *args):
        self.render("login.html", errors=None) 

    def post(self):
        login = self.get_argument('login')  # this will error if there's no login arg, iirc
        password = self.get_argument('password')

        if (password != 'sharing'):
            return self.render("login.html", errors='Wrong username or password.')

        self.set_secure_cookie('user', login)  

        self.redirect('/')


class AuthMixin(object):
    def get_current_user(self):
        return self.get_secure_cookie('user')
