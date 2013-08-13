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

application = flask.Flask(__name__)

basic_auth = BasicAuth(application)

application.config['BASIC_AUTH_USERNAME'] = 'virginia'
application.config['BASIC_AUTH_PASSWORD'] = 'sharing'


@application.route('/')
@basic_auth.required
def handle_request():
    dir1, dir2 = 'data1', 'data2'
    no_read_from = open("write_to.txt","r").read()
    if no_read_from == dir1:
	read_from = dir2
    else:
        read_from = dir1
    client = request.args.get('client_id')
    client = client.replace('=','%3D')
    all_clients = tool.query('select distinct client_id from campaigns')
    mappings = {encodeDES(int(i[0])): int(i[0]) for i in all_clients}
    try:
        client_id = mappings[client]
        client_name = tool.query("select name from clients where client_id='{0}'".format(client_id))
        client_name = client_name[0][0]
        _today = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_all_campaigns_day.txt'.format(read_from,client_id),'r')
        _today_data = _today.read()
        _today.close()
        aggregate = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_all_campaigns_aggregate.txt'.format(read_from,client_id),'r')
        aggregate_data = aggregate.read()
        aggregate.close()

        hourly_aggregate = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_hourly_aggregate.txt'.format(read_from,client_id),'r').read()
        daily_aggregate = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_daily_aggregate.txt'.format(read_from,client_id),'r').read()

        f1 = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_data_all.txt'.format(read_from,client_id),'r')
        all_data = f1.read()
        f1.close()
        f2 = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_data_day.txt'.format(read_from,client_id),'r')
        day_data = f2.read()
        f2.close()
        f3 = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_data_hourly.txt'.format(read_from,client_id),'r')
        hourly_data = f3.read()
        f3.close()
        f4 = open('/home/wes/Documents/analytics_and_reporting/reporting/{0}/client_{1}_data_monthly.txt'.format(read_from,client_id), 'r')
        monthly_data = f4.read()
        f4.close()
        today = strftime('%m') + '/' + strftime('%d') + '/' + strftime('%Y')

        return render_template("layout2.html", today_data=_today_data, aggregate_data=aggregate_data, hourly_aggregate=hourly_aggregate, daily_aggregate=daily_aggregate, all_data=all_data, day_data=day_data, hourly_data=hourly_data, monthly_data=monthly_data, client_name=client_name, today=today) 
    except KeyError:
        error = "Hit an error"
        return render_template("layout2.html", error=error)



if __name__ == '__main__':
    application.debug = True
    application.run()
