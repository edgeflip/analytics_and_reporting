#!/usr/bin/env python
import flask
from flask import request
from generate_report import generate_report_for_endpoint
import time
from flask import render_template
from time import strftime
import socket

app = flask.Flask(__name__)

@app.route('/',methods=['GET','POST'])
def report_table():
    if request.method == 'GET':
        client_id_hash = request.args.get('client_id')
        today = strftime('%m') + '/' + strftime('%d') + '/' + strftime('%Y')
        if client_id_hash == 'DXzVnCY4EZ0=':
            results_today_now, results_aggregate_now = generate_report_for_endpoint(2)
        elif client_id_hash == '49CA8T9bf4E=':
            results_today_now, results_aggregate_now = generate_report_for_endpoint(3)
        elif client_id_hash == 'H5m5nWhmBno=':
            results_today_now, results_aggregate_now = generate_report_for_endpoint(4)
        else:
            return "Your client_id doesn't match our records"
        visits, visits2 = str(results_today_now[0][0]), str(results_aggregate_now[0][0])
        clicks, clicks2 = str(results_today_now[0][1]), str(results_aggregate_now[0][1])
        auths, auths2 = str(results_today_now[0][2]), str(results_aggregate_now[0][2])
        distinct_auths, distinct_auths2 = str(results_today_now[0][3]), str(results_aggregate_now[0][3])
        users_shown, users_shown2 = str(results_today_now[0][4]), str(results_aggregate_now[0][4])
        users_shared, users_shared2 = str(results_today_now[0][5]), str(results_aggregate_now[0][5])
        friends_shared, friends_shared2 = str(results_today_now[0][6]), str(results_aggregate_now[0][6])
        distinct_friends_shared, distinct_friends_shared2 = str(results_today_now[0][7]), str(results_aggregate_now[0][7])
        clickbacks, clickbacks2 = str(results_today_now[0][8]), str(results_aggregate_now[0][8])
        return render_template("layout.html", today=today, visits=visits, clicks=clicks, auths=auths, distinct_auths=distinct_auths, users_shown=users_shown, users_shared=users_shared, friends_shared=friends_shared, distinct_friends_shared=distinct_friends_shared, clickbacks=clickbacks, visits2=visits2, clicks2=clicks2, auths2=auths2, distinct_auths2=distinct_auths2, users_shown2=users_shown2, users_shared2=users_shared2, friends_shared2=friends_shared2, distinct_friends_shared2=distinct_friends_shared2, clickbacks2=clickbacks2)

if __name__ == '__main__':
  app.run()
  #app.debug=True
  #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #sock.bind(('0.0.0.0',80))
  #port = sock.getsockname()[1]
  #sock.close()
  #app.run(host='0.0.0.0', port=port)



