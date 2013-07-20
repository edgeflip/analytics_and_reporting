#!/usr/bin/env python
from navigate_db import PySql
import MySQLdb as mysql
import os
import time
import datetime
from datetime import datetime
import urllib2
import httplib
import json

"""
run_queries takes a campaign_id, ideally Mayors for now, and is written in a way to accommodate it's daily execution.  Each day, or however often, run_queries is executed the algorithm attempts to open the file "timestamp.txt" and read it's contents. If the "timestamp.txt" file exists it's contents are read and assigned to a variable to limit the results we get.

"""




def run_queries(campaign_id):
    data_object = {"data": []}
    try:
        f = open('timestamp.txt','r')
        timestamp = f.read()
        f.close()
        os.remove('timestamp.txt')
        # assign the query to variable with string substitution of timestamp included
        events_query = "SELECT ip,fbid,friend_fbid,type,updated FROM events WHERE updated > FROM_UNIXTIME({0}) AND campaign_id={1}".format(timestamp,str(campaign_id))
    
    except IOError:
        events_query = "SELECT ip,fbid,friend_fbid,type,updated FROM events WHERE campaign_id={0}".format(str(campaign_id))

    con = mysql.connect('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb', 'edgeflip')
    cur = con.cursor()
    our_orm = PySql(cur)
    # this query will get all events with the pertinent campaign_id and, if provided, the
    # pertinent timestamp
    events = our_orm.query(events_query)
    # get all primary fbids from our query
    primaries = list(set([event[1] for event in events if event[1] != None]))
    for i in range(len(primaries)):
        data_object["data"].append({})
        data_object["data"][i][primaries[i]] = {}
        # query db for info we want on our primary
        primary_query = "SELECT fname,lname,email,gender,birthday,city,state FROM users WHERE fbid=%s" % primaries[i]
        primary_info = our_orm.query(primary_query)
        # assign the queried items as values to pertinent keys in our data object
        data_object["data"][i][primaries[i]]["fname"] = primary_info[0][0]
        data_object["data"][i][primaries[i]]["lname"] = primary_info[0][1]
        data_object["data"][i][primaries[i]]["email"] = primary_info[0][2]
        data_object["data"][i][primaries[i]]["gender"] = primary_info[0][3]
        data_object["data"][i][primaries[i]]["birthday"] = primary_info[0][4]
        data_object["data"][i][primaries[i]]["city"] = primary_info[0][5]
        data_object["data"][i][primaries[i]]["state"] = primary_info[0][6]
        
        data_object["data"][i][primaries[i]]["actions"] = {}
        # get all types of action associated with our primary
        cur_primary_event_types = [event[3] for event in events if event[1] == primaries[i]]
        # make a set of those actions so we can count them
        set_of_primary_event_types = set(cur_primary_event_types)
        count_of_event_types = {}
        # count the actions and update our data object accordingly
        count_of_event_types = {action: cur_primary_event_types.count(action) for action in set_of_primary_event_types}
        for action in set_of_primary_event_types:
            count_of_event_types[action] = cur_primary_event_types.count(action)
        data_object["data"][i][primaries[i]]["actions"].update(count_of_event_types)
    f = open('timestamp.txt','w')
    current_time = int(round(time.time()))
    f.write(str(current_time))
    f.close()
    # in case we've run our queries and nothing new has been found
    if len(data_object["data"]) == 0:
        # format the timestamp
        string_timestamp = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        print "No new data since %s has been found" % string_timestamp
    return data_object


# run all the queries necessary and generate a dictionary containing everything we need to send
# to mayors, as well as creating a timestamp file that will be read the next time around

def run_queries2(campaign_id):
    data_object = {"data":[]}
    try:
        time_file = open('timestamp.txt','r')
        timestamp = time_file.read()
        time_file.close()
        os.remove('timestamp.txt')
        query = "SELECT fbid,friend_fbid,type,udpated FROM events WHERE updated > FROM_UNIXTIME({0}) AND campaign_id={1}".format(timestamp,str(campaign_id))

    except IOError:
        query = "SELECT fbid,friend_fbid,type,updated FROM events WHERE campaign_id={0}".format(str(campaign_id))

    con = mysql.connect('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
    cur = con.cursor()
    our_orm = PySql(cur)
    # use the query defined above to get the events
    all_pertinent_events = our_orm.query(query)
    if all_pertinent_events != None:
        p_events = [event for event in all_pertinent_events if event[0] != None]
    
        for i in range(len(p_events)):
            data_object["data"].append({})
            data_object["data"][i][p_events[i][0]] = {}
            primary_query = "SELECT fname,lname,email,gender,birthday,city,state FROM users WHERE fbid={0}".format(p_events[i][0])
            primary_info = our_orm.query(primary_query)
            # add key value pairs of all our primary's personal data
            data_object["data"][i][p_events[i][0]]["fname"] = primary_info[0][0]
            data_object["data"][i][p_events[i][0]]["lname"] = primary_info[0][1]
            data_object["data"][i][p_events[i][0]]["email"] = primary_info[0][2]
            data_object["data"][i][p_events[i][0]]["gender"] = primary_info[0][3]
            data_object["data"][i][p_events[i][0]]["birthday"] = str(primary_info[0][4])
            data_object["data"][i][p_events[i][0]]["city"] = primary_info[0][5]
            data_object["data"][i][p_events[i][0]]["state"] = primary_info[0][6]
            # action data
            data_object["data"][i][p_events[i][0]]["action"] = {}
#            data_object["data"][i][p_events[i]]["action"][p_events[i][2]] = {}
            #data_object["data"][event[0]]["action"][event[2]][
            
            # secondary handling
            if p_events[i][1] != None:
                data_object["data"][i][p_events[i][0]]["action"][p_events[i][2]] = {}
                s_query = "SELECT fname,lname,email,gender,birthday,city,state FROM users WHERE fbid={0}".format(p_events[i][1])
                s_info = our_orm.query(s_query)

                data = {p_events[i][1]: {"fname": s_info[0][0], "lname": s_info[0][1], "email": s_info[0][2], "gender": s_info[0][3], "birthday": str(s_info[0][4]), "city": s_info[0][5], "state": s_info[0][6]}}
                data_object["data"][i][p_events[i][0]]["action"][p_events[i][2]].update(data)
            
            else:
                data_object["data"][i][p_events[i][0]]["action"][p_events[i][2]] = None
    else:
        pass
    f = open('timestamp.txt','w')
        current_time = int(round(time.time()))
        f.write(str(current_time))
        f.close()
        # in case we've run our queries and nothing new has been found
        if len(data_object["data"]) == 0:
                # format the timestamp
                string_timestamp = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                print "No new data since %s has been found" % string_timestamp

    # necessary for ActionKit
    data_object["page"] = "TargetedShareTest"
    data_object["email"] = "wes@edgeflip.com"    
    return data_object


def send_to_actionkit():
    headers = {"Content-Type": "application/json; charset=utf-8"}
    con = httplib.HTTPConnection("maig.actionkit.com")
    new_data = run_queries2(3)
    jsoned_data = json.dumps(new_data)
    con.request("POST", "/rest/v1/action/", jsoned_data, headers)    
    response = con.getresponse()
    print response.status, response.reason


if __name__ == '__main__':
    send_to_actionkit()    
