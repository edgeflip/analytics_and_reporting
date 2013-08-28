#!/usr/bin/env python
import sqlite3
from generate_report import create_unix_time_for_each_day
from generate_report import get_campaign_stuff_for_client
import datetime, time
from generate_report import all_hour_query
import json
from navigate_db import PySql
from time import strftime

# should be able to just do
# from models import CampaignSum, DaySum and use them

f = open('dbcreds.txt', 'r')
d = f.read().split('\n')
f.close()

tool = PySql(d[0], d[1], d[2], d[3])
tool.connect()


"""
    Built test models in sqlite3
"""

class Client(object):
    def __init__(self, client_id):
        self.client_id = client_id
        self.campaigns = []
        for camp_name in tool.query("select name from campaigns where client_id='%s'" % str(client_id)):
            self.campaigns.append(camp_name[0])
        self._conn = sqlite3.connect('schema.db', check_same_thread=False)
        self.c = self._conn.cursor()
    
    def get_campaigns(self):
        return self.campaigns

    # retrieve_data will get the data from the database for the pertinent client and build out the object
    # we've seen before
    # {"campaign": {"days": {"day": [visits, clicks, ...], "day": [visits, clicks...] }, "hours": {"day": [hour1, visits, clicks....], [hour2, visits, clicks...], .... [hour23, visits, clicks...] } } }
    def retrieve_data(self):
        data = {}
        for campaign in self.campaigns:
            #data[campaign] = {"days": {}, "hours": {}}
             
            results = self.c.execute("select data from campsum where campaign=?",(campaign,))
            self._conn.commit()
            results = results.fetchall()
            if len(results) > 0:
                data[campaign] = {"days": {}, "hours": {}}
                data[campaign]["days"] = json.loads(results[0][0])
        for campaign in data.keys():
            for day in data[campaign]["days"].keys():
                results = self.c.execute("select data from daysum where campaign=? and day=?", (campaign, day))
                data[campaign]["hours"][day] = json.loads(results.fetchall()[0][0])
        return data


class CampaignSum(object):
    def __init__(self):
        self._conn = sqlite3.connect('schema.db', check_same_thread=False)
        self.c = self._conn.cursor()

    def build(self, campaign, data):
        self._campaign = campaign
        self._data = json.dumps(data)

    def save(self):
        try:
            self.c.execute("insert into campsum values (?,?)", (self._campaign, self._data))
            self._conn.commit()
        except:
            print "CampaignSum object not built"

    def retrieve(self):
        pass

class DaySum(object):
    def __init__(self):
        self._conn = sqlite3.connect('schema.db', check_same_thread=False)
        self.c = self._conn.cursor()	

    def build(self, campaign, day, data):	
        self._campaign = campaign
        self._day = day
        self._data = json.dumps(data)

    def save(self):
        try:
            self.c.execute("insert into daysum values (?,?,?)", (self._campaign, self._day, self._data))
            self._conn.commit()
        except:
            print "DaySum object not built"



def make_all_object():
    all_campaigns = tool.query("select client_id, name from clients") 
    days_for_month = create_unix_time_for_each_day()
    days_for_month = [ datetime.datetime.fromtimestamp(d) for d in days_for_month ]
    all_data = all_hour_query()
    our_object = {}
    for client in all_campaigns:
        client_id = client[0]
        client_name = client[1]
        our_object[client_name] = {}
        campaigns = get_campaign_stuff_for_client(client_id)
        for campaign in campaigns:
            our_object[ client_name ][ campaign[1] ] = {}
            our_object[ client_name ][ campaign[1] ]["days"] = {}
            our_object[ client_name ][ campaign[1] ]["hours"] = {}
            # get all data that is for this campaign
            this_campaign_data = [ _set for _set in all_data if _set[0] == campaign[0] ]
            days_we_have = list( set( [ str( datetime.datetime(int(e[1]), int(e[2]), int(e[3])) ) for e in this_campaign_data ] ) )
            not_accounted_days = [
                                     str(datetime.datetime(d.year, d.month, d.day))
                                     for d in days_for_month if str(datetime.datetime(d.year, d.month, d.day)) not in days_we_have
                                 ]
            for day in days_we_have:
                # the day data for each day
                day_data = [
                               e for e in [
                                              j[5:] for j in this_campaign_data if str(datetime.datetime(int(j[1]), int(j[2]), int(j[3]))) == day
                                          ]
                           ]

                day_data_new = []
                for each in day_data:
                    day_data_new.append( [ int(j) for j in each ] )
                sums = []
                for i in range( len( day_data_new[0] ) ):
                    sums.append( sum([ x[i] for x in day_data_new ]) )
                our_object[ client_name ][ campaign[1] ]["days"][day] = sums
                # hour data portion
                hour_data = [
                                e for e in [
                                               j[4:] for j in this_campaign_data if str(datetime.datetime(int(j[1]), int(j[2]), int(j[3]))) == day
                                           ]
                            ]
                hour_data_new = []
                # convert our days to integers
                for each in hour_data:
                    hour_data_new.append( [ int(j) for j in each ] )
                for i in range(24):
                    if i not in [e[0] for e in hour_data_new]:
                        hour_data_new.append([i] + [0 for j in range(9)])

                #hour_data_new += [ [i] + [0 for j in range(9)] for i in range(24) if i not in [e[0] for e in hour_data_new] ] 
                our_object[ client_name ][ campaign[1] ]["hours"][day] = hour_data_new
            # for all the days over the past month that we don't have data for for the current iteration's campaign...
            for day in not_accounted_days:
                our_object[ client_name ][ campaign[1] ]["days"][day] = [ 0 for i in range(9) ]
                hour_data = [ [j] + [0 for i in range(9)] for j in range(24) ]
                our_object[ client_name ][ campaign[1] ]["hours"][day] = hour_data
    # port data to django models
    #return our_object 
    for client in our_object.keys():
        for campaign in our_object[client].keys():
            ddata = our_object[client][campaign]["days"]
        #    for k in ddata.keys():
        #        if sum(ddata[k]) == 0:
        #            del ddata[k]

            C = CampaignSum()
            C.save()

            for day in our_object[client][campaign]["hours"].keys():
                hdata = our_object[client][campaign]["hours"][day]

            # if [sum(row) for row in hdata] == range(24): continue
            # d = datetime.strptime(d, "%Y-%m-%d %H:%M:%S")

                D = DaySum()
                D.save()
    print "Data successfully ported to Django Models"



"""
    This will likely be the function that is called periodically via a cron after the initial
    data scrape of all our clients and campaigns respectively.  A sort of moving window of 
    30 days of data for all campaigns and clients
"""

def keep_updated():
    this_month = create_unix_time_for_each_day()
    this_month = [ time.mktime(datetime.datetime(j.year, j.month, j.day).timetuple()) for j in [datetime.datetime.fromtimestamp(i) for i in this_month] ]
    cur_hour = strftime('%H')
    clients = tool.query("select client_id, name from clients")
    query_since = 0
    for client_id, name in clients: 
        campaigns = get_campaign_stuff_for_client(client_id)
        # call the Client class's retrieve_data method
        cur_data = Client(client_id).retrieve_data()
        # get the current month of days stored in the db
        cur_month_stored = [ i for i in cur_data[campaigns[0][1]]['days'].keys() ]
        cur_month_stored = [ time.strptime(i, "%Y-%m-%d %H:%M:%S") for i in cur_month_stored ]
        cur_month_stored = [ time.mktime(datetime.datetime(j.tm_year, j.tm_mon, j.tm_mday).timetuple()) for j in cur_month_stored ]
        new_times = [i for i in this_month if i not in cur_month_stored]
        latest_day = max(cur_month_stored)
        latest_day = str(datetime.datetime.fromtimestamp(latest_day))
        hours = [j[0] for j in cur_data[campaigns[0][1]]['hours'][latest_day]]
        break
    if hours == range(24):
        pass
    elif len(new_times) > 0:
        new_times = min(new_times)
    new_data = tool.query(main_query_hour_by_hour_new.format(new_times))
    return new_data 
