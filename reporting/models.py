#!/usr/bin/env python
import sqlite3, json
from navigate_db import PySql


f = open('dbcreds.txt', 'r')
d = f.read().split('\n')
f.close()

tool = PySql(d[0], d[1], d[2], d[3])
tool.connect()

# MODELS FOR THE DASHBOARD DATA 


class Client(object):
    def __init__(self, client_id):
        self.client_id = client_id
        self.campaigns = []
        for campaign_id, campaign_name in get_campaign_stuff_for_client(client_id):
            self.campaigns.append((campaign_id, campaign_name))
        #for camp_name in tool.query("select name from campaigns where client_id='%s'" % str(client_id)):
        #    self.campaigns.append(camp_name[0])
        self._conn = sqlite3.connect('schema.db', check_same_thread=False)
        self.c = self._conn.cursor()

    def get_campaigns(self):
        return self.campaigns

    # retrieve_data will get the data from the database for the pertinent client and build out the object
    # we've seen before
    # {"campaign": {"days": {"day": [visits, clicks, ...], "day": [visits, clicks...] }, "hours": {"day": [hour1, visits, clicks....], [hour2, visits, clicks...], .... [hour23, visits, clicks...] } } }
    def retrieve_data(self):
        data = {}
        for campaign_id, campaign in self.campaigns:
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

def get_campaign_stuff_for_client(client_id):
    res = tool.query("select campaign_id, name from campaigns where client_id='{0}' and campaign_id in (select distinct campaign_id from events where type='button_load')".format(client_id))
    return res

