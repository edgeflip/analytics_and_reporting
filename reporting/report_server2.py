#!/usr/bin/env python
import csv
import flask
from flask import request
from flask import render_template
from time import strftime
import socket
import json

application = flask.Flask(__name__)

@application.route('/',methods=['GET','POST'])
def main_handler():
    if request.method == 'GET':
        try:
            client_id_hash = request.args.get('client_id')
            if client_id_hash == 'DXzVnCY4EZ0=':
                from hourly_data_va import hourly_data
                from daily_data_va import daily_data
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
    application.debug = True
    application.run()
#    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#    sock.bind(('0.0.0.0',80))
#    port = sock.getsockname()[1]
#    sock.close()
#    application.run(host='0.0.0.0',port=port)
