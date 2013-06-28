#!/usr/bin/env python
import flask
from flask import request
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import json
import logging
import requests

app = flask.Flask(__name__)

"""

        Flask application with endpoints to subscribe our app to the Realtime Updates API
        so we will be notified by facebook when our users update their news feed.  This data
        will be stored in an s3 bucket called "fbrealtime" and that bucket will be persistently
        checked for updates by our crawler.  The data the crawler finds within the bucket
        will be extracted, utilized, and then deleted.  The data we get from using the
        "fbrealtime" data will be stored in another s3 bucket called "fbcrawl1" containing
        all our users' news feeds.

"""

VERIFY_TOKEN = 'someverifytokendoesntmatterwhatitis'


def subscribe_app():
        global VERIFY_TOKEN
        import urllib2, urllib
        FB_CLIENT_ID = '471727162864364'
        # access_token is sent as a query string parameter
        APP_ACCESS_TOKEN = '471727162864364|jVJvD2JfB6iwFvx9evMLOgLQiNg'

        # object, fields, callback_url, and verify_token are sent as urllib.urlencode([('param','val')])
        #CALLBACK_URL = 'http://ec2-50-16-226-172.compute-1.amazonaws.com:5000'
        CALLBACK_URL = 'http://50.16.226.172:5000'

        subscribe_api = "https://graph.facebook.com/{0}/subscriptions?access_token={1}&object=user&fields=feed&verify_token={2}&callback_url={3}".format(FB_CLIENT_ID, APP_ACCESS_TOKEN, VERIFY_TOKEN, CALLBACK_URL)

        r = requests.post(subscribe_api)
        return r.text


@app.route('/', methods=['GET','POST'])
def subscription_app():
        global VERIFY_TOKEN
        import threading
        if request.method == 'GET':
                first = request.args.get('hub.mode')
                second = request.args.get('hub.challenge')
                third = request.args.get('hub.verify_token')
                _all = request.query_string
                size = len('hub.challenge')
                start = _all.find('hub.challenge') + size + 1
                stop = _all.find('&', start)
                challenge = _all[start:stop]
                if first == 'subscribe' and third == VERIFY_TOKEN:
                        print '\n\n\n' +challenge + '\n' + str(threading.active_count()) + '\n\n'
                        return challenge

        # we will be receiving posts from facebook containing our user object's feed updates and the time at which they were made
        elif request.method == 'POST':
                conn = S3Connection('', '')
                # fbrealtime bucket
                bucket = conn.get_bucket('fbrealtime')
                # debating whether or not to use timestamp key
                # this_key = int(time.time())
                # new_key = bucket.new_key()
                # new_key.key = this_key
                # new_key.set_contents_from_string(data)

                # the JSON from facebook
                data = json.dumps(json.loads(request.data))
                logging.info('RECEIVED DATA FROM FACEBOOK')
                print "We received some update data!"


@app.route('/subscribe')
def subscribe():
        return subscribe_app()


if __name__ == '__main__':
        app.debug = True
        app.run(host='0.0.0.0')
                                     
