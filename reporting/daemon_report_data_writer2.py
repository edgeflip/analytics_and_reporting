#!/usr/bin/env python
from generate_report import new_query
from generate_report import get_campaign_stuff_for_client
from generate_report import new_hour_query
from handle_ec2_time_difference import handle_time_difference
import json
from time import strftime

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
        clients_data_all_str = json.dumps(clients_data_all)
        clients_data_day_str = json.dumps(clients_data_day)
        clients_data_hourly_str = json.dumps(clients_hourly_data)
        all_data_file = 'client_{0}_data_all.txt'.format(client)
        day_data_file = 'client_{0}_data_day.txt'.format(client)
        hourly_data_file = 'client_{0}_data_hourly.txt'.format(client)
        f1 = open(all_data_file,'w')
        f1.write(clients_data_all_str)
        f1.close()
        f2 = open(day_data_file,'w')
        f2.write(clients_data_day_str)
        f2.close()
        f3 = open(hourly_data_file, 'w')
        f3.write(clients_data_hourly_str)
        f3.close()
    print "data written"


if __name__ == '__main__':
    write_and_consume()
