#!/usr/bin/env python
import flask
import json
import csv
from flask import request
from generate_data_for_export_original import tool
from generate_report import encodeDES
from flask import render_template

application = flask.Flask(__name__)


@application.route('/')
def handle_request():
    client = request.args.get('client_id')
    client = client.replace('=','%3D')
    all_clients = tool.query("select distinct client_id from campaigns")
    mappings = {encodeDES(int(i[0])): int(i[0]) for i in all_clients}
    try:
        client_id = mappings[client]
        client_name = tool.query("select name from clients where client_id='{0}'".format(client_id))
        client_name = client_name[0][0]
        f1 = open('client_{0}_data_all.txt'.format(client_id),'r')
        all_data = f1.read()
        f1.close()
        f2 = open('client_{0}_data_day.txt'.format(client_id),'r')
        day_data = f2.read()
        f2.close()

        return render_template("layout2.html", all_data=all_data, day_data=day_data, client_name=client_name) 
    except KeyError:
        return render_template("layout2.html", error=error)



if __name__ == '__main__':
    application.debug = True
    application.run()
