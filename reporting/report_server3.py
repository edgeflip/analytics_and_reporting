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
        f = open('client_{0}_data.txt'.format(client_id),'r')
        data = f.read()
        f.close()
        return render_template("layout2.html", data=data, client_id=client_id) 
    except KeyError:
        return render_template("layout2.html", error=error)



if __name__ == '__main__':
    application.debug = True
    application.run()
