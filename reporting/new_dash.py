#!/usr/bin/env python
import flask
import json
import csv
from flask import request
from generate_data_for_export_original import tool
from generate_report import encodeDES
from flask import render_template
from time import strftime
from flask.ext.basicauth import BasicAuth
from con_s3 import connect_s3

application = flask.Flask(__name__)

basic_auth = BasicAuth(application)

application.config['BASIC_AUTH_USERNAME'] = 'virginia'
application.config['BASIC_AUTH_PASSWORD'] = 'sharing'


@application.route('/')
@basic_auth.required
def handle_request():
    conn = connect_s3()
    dash = conn.get_bucket('edgeflip_dashboard')
    dir1, dir2 = "1", "2"
    write_key = dash.get_key('write_to')
    write_to = write_key.get_contents_as_string()
    if write_to == dir1:
	read_from = dir2
    else:
	read_from = dir1
    client = request.args.get('client_id')
    client = client.replace('=','%3D')
    all_clients = tool.query("select distinct client_id from campaigns")
    mappings = {encodeDES(int(i[0])): int(i[0]) for i in all_clients}
    try:
        client_id = mappings[client]
        client_name = tool.query("select name from clients where client_id='{0}'".format(client_id))
        client_name = client_name[0][0]
	_today = dash.get_key('{0}_client_{1}_all_campaigns_day'.format(read_from, client_id))
	_today_data = _today.get_contents_as_string()
        aggregate = dash.get_key('{0}_client_{1}_all_campaigns_aggregate'.format(read_from, client_id))
	aggregate_data = aggregate.get_contents_as_string()
        hourly_aggregate_key = dash.get_key('{0}_client_{1}_hourly_aggregate'.format(read_from, client_id))
        hourly_aggregate = hourly_aggregate_key.get_contents_as_string()
        daily_aggregate_key = dash.get_key('{0}_client_{1}_daily_aggregate'.format(read_from, client_id))
        daily_aggregate = daily_aggregate_key.get_contents_as_string()
        f1 = dash.get_key('{0}_client_{1}_data_all'.format(read_from, client_id))
        all_data = f1.get_contents_as_string()
        f2 = dash.get_key('{0}_client_{1}_data_day'.format(read_from, client_id))
        day_data = f2.get_contents_as_string()
        f3 = dash.get_key('{0}_client_{1}_data_hourly'.format(read_from, client_id))
        hourly_data = f3.get_contents_as_string()
        f4 = dash.get_key('{0}_client_{1}_data_monthly'.format(read_from, client_id))
        monthly_data = f4.get_contents_as_string()
        today = strftime("%m/%d/%Y")
        return render_template("new_temp.html", today_data=_today_data, aggregate_data=aggregate_data, hourly_aggregate=hourly_aggregate, daily_aggregate=daily_aggregate, all_data=all_data, day_data=day_data, hourly_data=hourly_data, monthly_data=monthly_data, client_name=client_name, today=today) 
    except KeyError:
        error = "Hit an error"
        return render_template("new_temp.html", error=error)


if __name__ == '__main__':
    application.debug = True
    application.run()
