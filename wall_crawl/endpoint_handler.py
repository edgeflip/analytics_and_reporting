#!/usr/bin/env python
import flask
from flask import request
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import json
import logging
import requests
from werkzeug.contrib.fixers import ProxyFix
import csv
import time

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

VERIFY_TOKEN = 'blahblahblah'

def subscribe_app():
    global VERIFY_TOKEN
    FB_CLIENT_ID = '471727162864364'
    # access_token is sent as a query string parameter
    APP_ACCESS_TOKEN = '471727162864364|jVJvD2JfB6iwFvx9evMLOgLQiNg'
    # object, fields, callback_url, and verify_token are sent as urllib.urlencode([('param','val')])
    CALLBACK_URL = 'http://50.16.226.172:5000'
    payload_url = "https://graph.facebook.com/{0}/subscriptions".format(FB_CLIENT_ID)
    payload = {"access_token": APP_ACCESS_TOKEN, "object": "user", "fields": "feed", "verify_token": VERIFY_TOKEN, "callback_url": CALLBACK_URL}    
    r = requests.post(payload_url, data=payload)
    return r.text



@app.route('/', methods=['GET','POST'])
def subscription_app():
    global VERIFY_TOKEN
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
        return challenge

    # we will be receiving posts from facebook containing our user object's feed updates and the time at which they were made
    elif request.method == 'POST':
        data = json.loads(request.data)
        conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
        # fbrealtime bucket
        bucket = conn.get_bucket('fbrealtime')
        # get the data that we currently have
        to_add = []
        for entry in data['entry']:
            fbid = entry['id']
            updated_time = entry['time']
            to_add.append((fbid,updated_time)) 

        cur_data = {'data': to_add}
        cur_data_str = json.dumps(cur_data)
        data_key = bucket.new_key()
        _time = str(int(time.time()))
        data_key.key = _time
        data_key.set_contents_from_string(cur_data_str)
        # keep a log file and write to it
        with open('realtime_log.txt','ab') as f:
            f.seek(0, os.SEEK_END)
            f.write(cur_data_str+'\n')
            f.close()
            return    
        #print "We received some update data!"


@app.route('/check_subscriptions')
def check_subscription():
    FB_CLIENT_ID = '471727162864364'
    APP_ACCESS_TOKEN = '471727162864364|jVJvD2JfB6iwFvx9evMLOgLQiNg'
    subscribe_api = "https://graph.facebook.com/{0}/subscriptions?access_token={1}".format(FB_CLIENT_ID, APP_ACCESS_TOKEN)
    r = requests.get(subscribe_api)
    return r.text

@app.route('/subscribe')
def subscribe():
    return subscribe_app()


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
