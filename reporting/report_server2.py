#!/usr/bin/env python
# LOCAL VERSION
# NEED TO UNCOMMENT THE SOCKET BINDS IN if __name__ == '__main__' FOR LIVE USE
import csv
import flask
from flask import request
from flask import render_template
from time import strftime
import socket
import json
from flask.ext.basicauth import BasicAuth

#application.config['BASIC_AUTH_PASSWORD'] = 'password'


application = flask.Flask(__name__)

application.config['BASIC_AUTH_USERNAME'] = 'virginia'
application.config['BASIC_AUTH_PASSWORD'] = 'sharing'

basic_auth = BasicAuth(application)

@application.route('/',methods=['GET','POST'])
@basic_auth.required
def main_handler():
    if request.method == 'GET':
        try:
            client_id_hash = request.args.get('client_id')
            if client_id_hash == 'DXzVnCY4EZ0=':
                hourly_data = {'data': []}
                hour_reader = csv.reader(open('hourly_data_va.csv','r'))
                try:
                    while True:
                        hourly_data['data'].append([int(i) for i in hour_reader.next()])
                except StopIteration:
                    pass
                # read in daily_data
                daily_data = {'data': []}
                day_reader = csv.reader(open('daily_data_va.csv','r'))
                try:
                    while True:
                        daily_data['data'].append([int(i) for i in day_reader.next()])
                except StopIteration:
                    pass

                csvfile = open('current_data_va.csv','r')
                reader = csv.reader(csvfile,delimiter=',')
                first_row = reader.next()
                second_row = reader.next()
                 
                today = strftime('%m') + '/' + strftime('%d') + '/' + strftime('%Y')
                return render_template("layout.html", hourly_data=hourly_data, daily_data=daily_data, today=today, visits=str(int(first_row[0])), clicks=str(int(first_row[1])), auths=str(int(first_row[2])), distinct_auths=str(int(first_row[3])), users_shown=str(int(first_row[4])), users_shared=str(int(first_row[5])), friends_shared=str(int(first_row[6])), distinct_friends_shared=str(int(first_row[7])), clickbacks=str(int(first_row[8])), visits2=str(int(second_row[0])), clicks2=str(int(second_row[1])), auths2=str(int(second_row[2])), distinct_auths2=str(int(second_row[3])), users_shown2=str(int(second_row[4])), users_shared2=str(int(second_row[5])), friends_shared2=str(int(second_row[6])), distinct_friends_shared2=str(int(second_row[7])), clickbacks2=str(int(second_row[8])))
            else:
                return "We don't have any data for that client_id"

        except IOError:
            return "We don't have data yet"
    else:
        return "We don't handle those kinds of requests here"


if __name__ == '__main__':
    application.run()
    #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #sock.bind(('0.0.0.0',80))
    #port = sock.getsockname()[1]
    #sock.close()
    #application.run(host='0.0.0.0',port=port)
