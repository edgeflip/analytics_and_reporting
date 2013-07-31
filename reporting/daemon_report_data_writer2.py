#!/usr/bin/env python
from generate_report import new_query
from generate_report import get_campaign_stuff_for_client
from handle_ec2_time_difference import handle_time_difference
import json

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
            campaigns_alltime_data["%s" % i[1]] = [0 for i in range(9)]

        if i[0] in [e[0] for e in day_data]:
            campaigns_day_ago_data["%s" % i[1]] = [int(w) for w in [e for e in day_data if e[0] == i[0]][0]]
        else:
            campaigns_day_ago_data["%s" % i[1]] = [0 for i in range(9)]


    #campaigns_alltime_data = {"%s" % i[1]: [int(w) for w in [e for e in alltime_data if e[0] == i[0]][0]] for i in campaign_stuff}
    #campaigns_day_ago_data = {"%s" % i[1]: [int(w) for w in [e for e in day_data if e[0] == i[0]][0]] for i in campaign_stuff if i[0] in [e[0] for e in day_data]}
    return campaigns_alltime_data, campaigns_day_ago_data




def write_and_consume():
    from generate_data_for_export_original import tool
    all_clients = tool.query("select distinct client_id from campaigns")
    for each_client in all_clients:
        client = each_client[0]
        clients_data_all, clients_data_day = couple_data_with_info(client)
        clients_data_all_str = json.dumps(clients_data_all)
        clients_data_day_str = json.dumps(clients_data_day)
        all_data_file = 'client_{0}_data_all.txt'.format(client)
        day_data_file = 'client_{0}_data_day.txt'.format(client)
        f1 = open(all_data_file,'w')
        f1.write(clients_data_str)
        f1.close()
        f2 = open(day_data_file,'w')
        f2.write(clients_data_day_str)
        f2.close()
    print "data written"


if __name__ == '__main__':
    write_and_consume()
