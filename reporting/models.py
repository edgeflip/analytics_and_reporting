#!/usr/bin/env python
from dash_data import tool

# MODELS FOR THE DASHBOARD DATA 


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


