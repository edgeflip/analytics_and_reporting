#!/usr/bin/env python
from generate_report import new_query
from generate_report import get_campaign_stuff_for_client
import json

def couple_data_with_info(client_id):
    _all = new_query()
    campaign_stuff = get_campaign_stuff_for_client(client_id)
    campaigns = {"%s" % i[1]: [int(w) for w in [e for e in _all if e[0] == i[0]][0]] for i in campaign_stuff}
    return campaigns

def write_and_consume():
    from generate_data_for_export_original import tool
    all_clients = tool.query("select distinct client_id from campaigns")
    for each_client in all_clients:
        client = each_client[0]
        clients_data = couple_data_with_info(client)
        clients_data_str = json.dumps(clients_data)
        _file = 'client_{0}_data.txt'.format(client)
        f = open(_file,'w')
        f.write(clients_data_str)
        f.close()
    print "data written"


if __name__ == '__main__':
    write_and_consume()
