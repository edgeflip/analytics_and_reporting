#!/usr/bin/env python
from generate_report import new_query
from generate_report import get_campaign_stuff_for_client
from generate_report import new_hour_query
from generate_report import create_unix_time_for_each_day
from generate_report import new_month_query
from handle_ec2_time_difference import handle_time_difference
import json
import datetime
from time import strftime

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
def generate_hourly_object(client_id):
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


    #campaigns_alltime_data = {"%s" % i[1]: [int(w) for w in [e for e in alltime_data if e[0] == i[0]][0]] for i in campaign_stuff}
    #campaigns_day_ago_data = {"%s" % i[1]: [int(w) for w in [e for e in day_data if e[0] == i[0]][0]] for i in campaign_stuff if i[0] in [e[0] for e in day_data]}
    return campaigns_alltime_data, campaigns_day_ago_data




def write_and_consume():
    from generate_data_for_export_original import tool
    all_clients = tool.query("select distinct client_id from campaigns")
    for each_client in all_clients:
        client = each_client[0]
        clients_data_all, clients_data_day = couple_data_with_info(client)
        clients_hourly_data = generate_hourly_object(client)
        clients_monthly_data = create_month_object(client)
        clients_data_all_str = json.dumps(clients_data_all)
        clients_data_day_str = json.dumps(clients_data_day)
        clients_data_hourly_str = json.dumps(clients_hourly_data)
        clients_data_monthly_str = json.dumps(clients_monthly_data)
        all_data_file = 'client_{0}_data_all.txt'.format(client)
        day_data_file = 'client_{0}_data_day.txt'.format(client)
        hourly_data_file = 'client_{0}_data_hourly.txt'.format(client)
        monthly_data_file = 'client_{0}_data_monthly.txt'.format(client)
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
    print "data written"


if __name__ == '__main__':
    write_and_consume()
