#!/usr/bin/env python
from generate_report import make_hour_by_hour_object
from generate_report import make_day_by_day_object
from generate_report import generate_report_for_endpoint_new
from generate_report import new_query
from generate_report import get_campaign_stuff_for_client
from generate_report import new_hour_query
from generate_report import create_unix_time_for_each_day
from generate_report import new_month_query
from handle_ec2_time_difference import handle_time_difference
import json
import datetime, time
from time import strftime
import os


"""
    takes a client_id and gets all of the campaigns running for that client
    then takes what is returned from mysql and gets what is needed from there,
    also uses fancy timestamp stuff to handle the campaigns that have missing
    days from the past month and creates a full object
"""

# functionality that will create an object that adds rows for non existing clients and days
# the days and clients that don't exist will be filled with a day in the form of a unix timestamp
# so that the JavaScript can do it's magic with new Date(unix_timestamp * 1000); .....
def create_month_object(client_id):
    month_data = new_month_query()
    unix_times = create_unix_time_for_each_day()
    campaign_stuff = get_campaign_stuff_for_client(client_id)
    obj = {}
    stringified_dates = [str(datetime.datetime.fromtimestamp(i).date()) for i in unix_times]
    for campaign in campaign_stuff:
        # for each campaign check which days the user has filled
        campaign_accounted_days = [str(datetime.datetime(int(e[1]), int(e[2]), int(e[3])).date()) for e in month_data if e[0] == campaign[0]]
        # not_accounted_days will be iterated through to generate the days that we don't ahve for our object
        not_accounted_days = [e for e in stringified_dates if e not in campaign_accounted_days]
        obj[campaign[1]] = [[str(datetime.datetime(int(e[1]),int(e[2]),int(e[3])).date())]+[int(j) for j in e[4:]] for e in month_data if e[0] == campaign[0]]
        for day in not_accounted_days:
            to_append = [day] + [0 for i in range(9)]
            obj[campaign[1]].append(to_append)
    # sort the stuff
    # we are going to have to use the datetime, time, and strftime modules
    for key in obj.keys():
        for i in range(len(obj[key])):
            for j in range(len(obj[key])-1):
                first_parts = obj[key][j][0].split('-')
                first = time.mktime(datetime.datetime(int(first_parts[0]), int(first_parts[1]), int(first_parts[2])).timetuple())
                second_parts = obj[key][j+1][0].split('-')
                second = time.mktime(datetime.datetime(int(second_parts[0]), int(second_parts[1]), int(second_parts[2])).timetuple())
                if first > second:
                    tmp = obj[key][j]
                    obj[key][j] = obj[key][j+1]
                    obj[key][j+1] = tmp
    return obj


"""
    takes a client_id and gets all the campaigns running for that client
    the mysql_stuff gets all the data from midnight of today until now 
    and the get_campaign_stuff_for_client gets all the campaigns currently
    running for the parameterized client so we can build an object and not
    forget to include said campaign just because we don't have any data
    returned for them.  we build an object that includes all campaigns and
    if we don't have data for that campaign we just substitute zeros for
    that campaign for each hour
"""
def create_hour_object(client_id):
    current_hour = int(strftime('%H'))
    obj = {}
    mysql_stuff = new_hour_query()
    campaign_stuff = get_campaign_stuff_for_client(client_id)
    for campaign in campaign_stuff:
        obj[campaign[1]] = [[int(j) for j in e[1:]] for e in mysql_stuff if e[0] == campaign[0]]
        for hour in range(0, current_hour):
            if hour not in [w[0] for w in obj[campaign[1]]]:
                new_list = [hour] + [0 for i in range(9)]
                obj[campaign[1]].append(new_list)
    for key in obj.keys():
        for i in range(len(obj[key])):
            for j in range(len(obj[key])-1):
                if obj[key][j][0] > obj[key][j+1][0]:
                    tmp = obj[key][j]
                    obj[key][j] = obj[key][j+1]
                    obj[key][j+1] = tmp
    return obj


"""
    couple_data_with_info grabs all the campaigns for a parameterized client
    along with all the data for each campaign and builds an object that couples
    the data for that campaign with that campaigns name as the key to the objects
    that are returned by the method
"""


def couple_data_with_info(client_id):
    day_ago = handle_time_difference()
    alltime_data = new_query(0)
    day_data = new_query(day_ago)
    campaign_stuff = get_campaign_stuff_for_client(client_id)
    campaigns_alltime_data = {}
    campaigns_day_ago_data = {}
    for i in campaign_stuff:
        if i[0] in [e[0] for e in alltime_data]:
            campaigns_alltime_data["%s" % i[1]] = [int(w) for w in [e for e in alltime_data if e[0] == i[0]][0]]
        else:
            # we got none 
            campaigns_alltime_data["%s" % i[1]] = [0 for e in range(10)]

        if i[0] in [e[0] for e in day_data]:
            campaigns_day_ago_data["%s" % i[1]] = [int(w) for w in [e for e in day_data if e[0] == i[0]][0]]
        else:
            campaigns_day_ago_data["%s" % i[1]] = [0 for e in range(10)]

    return campaigns_alltime_data, campaigns_day_ago_data




def write_and_consume():
    dir1,dir2 = "data1", "data2"
    write_to = open("/home/ubuntu/reporting_app/write_to.txt","r").read()
    write_to = write_to.split('\n')[0]
    from generate_data_for_export_original import tool
    all_clients = tool.query("select distinct client_id from campaigns")
    for each_client in all_clients:
        client = each_client[0]
        # aggregate table data
        today_all_campaigns, aggregate_all_campaigns = generate_report_for_endpoint_new(client)
        # aggregate line chart data
        hourly_all_campaigns = make_hour_by_hour_object(client)
        daily_all_campaigns = make_day_by_day_object(client)

        today_all_campaigns_json = json.dumps({"data": today_all_campaigns})
        aggregate_all_campaigns_json = json.dumps({"data": aggregate_all_campaigns})
        clients_data_all, clients_data_day = couple_data_with_info(client)
        clients_hourly_data = create_hour_object(client)
        clients_monthly_data = create_month_object(client)
        clients_data_all_str = json.dumps(clients_data_all)
        clients_data_day_str = json.dumps(clients_data_day)
        clients_data_hourly_str = json.dumps(clients_hourly_data)
        clients_data_monthly_str = json.dumps(clients_monthly_data)
	today_all_campaigns = '/home/ubuntu/reporting_app/{0}/client_{1}_all_campaigns_day.txt'.format(write_to,client)
	aggregate_all_campaigns = '/home/ubuntu/reporting_app/{0}/client_{1}_all_campaigns_aggregate.txt'.format(write_to,client)
	hour_by_hour_all_campaigns = '/home/ubuntu/reporting_app/{0}/client_{1}_hourly_aggregate.txt'.format(write_to,client)
	day_by_day_all_campaigns = '/home/ubuntu/reporting_app/{0}/client_{1}_daily_aggregate.txt'.format(write_to,client)
	all_data_file = '/home/ubuntu/reporting_app/{0}/client_{1}_data_all.txt'.format(write_to,client)
	day_data_file = '/home/ubuntu/reporting_app/{0}/client_{1}_data_day.txt'.format(write_to,client)
	hourly_data_file = '/home/ubuntu/reporting_app/{0}/client_{1}_data_hourly.txt'.format(write_to,client)
	monthly_data_file = '/home/ubuntu/reporting_app/{0}/client_{1}_data_monthly.txt'.format(write_to,client)
        _today = open(today_all_campaigns,'w')
        _today.write(today_all_campaigns_json)
        _today.close()
        _aggregate = open(aggregate_all_campaigns,'w')
        _aggregate.write(aggregate_all_campaigns_json)
        _aggregate.close()
        hr_by_hr = open(hour_by_hour_all_campaigns, 'w')
        hr_by_hr.write(json.dumps(hourly_all_campaigns))
        hr_by_hr.close()
        d_by_d = open(day_by_day_all_campaigns, 'w')
        d_by_d.write(json.dumps(daily_all_campaigns))
        d_by_d.close()
        f1 = open(all_data_file,'w')
        f1.write(clients_data_all_str)
        f1.close()
        f2 = open(day_data_file,'w')
        f2.write(clients_data_day_str)
        f2.close()
        f3 = open(hourly_data_file, 'w')
        f3.write(clients_data_hourly_str)
        f3.close()
        f4 = open(monthly_data_file,'w')
        f4.write(clients_data_monthly_str)
        f4.close()
    if write_to == dir1:
	new_write = dir2
    else:
	new_write = dir1
    os.remove("/home/ubuntu/reporting_app/write_to.txt")
    _new = open("/home/ubuntu/reporting_app/write_to.txt","w")
    _new.write(new_write)
    _new.close()
    print "data written"


if __name__ == '__main__':
    write_and_consume()
