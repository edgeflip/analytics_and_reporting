#!/usr/bin/env python
import sqlite3
from generate_report import create_unix_time_for_each_day
from generate_report import get_campaign_stuff_for_client
import datetime, time
from generate_report import all_hour_query
import json


# should be able to just do
# from models import CampaignSum, DaySum and use them


"""
    Built test models in sqlite3
"""

class CampaignSumMock(object):
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

class DaySumMock(object):
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


def make_all_object(client_id):
    days_for_month = create_unix_time_for_each_day()
    days_for_month = [ datetime.datetime.fromtimestamp(d) for d in days_for_month ]
    all_data = all_hour_query()
    our_object = {}
    campaigns = get_campaign_stuff_for_client(client_id)
    for campaign in campaigns:
        our_object[ campaign[1] ] = {}
        our_object[ campaign[1] ]["days"] = {}
        our_object[ campaign[1] ]["hours"] = {}
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
            our_object[ campaign[1] ]["days"][day] = sums
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
            our_object[ campaign[1] ]["hours"][day] = hour_data_new
        # for all the days over the past month that we don't have data for for the current iteration's campaign...
        for day in not_accounted_days:
            our_object[ campaign[1] ]["days"][day] = [ 0 for i in range(9) ]
            hour_data = [ [j] + [0 for i in range(9)] for j in range(24) ]
            our_object[ campaign[1] ]["hours"][day] = hour_data

    # port data to django models
    
    for c in our_object.keys():
        ddata = our_object[c]["days"]
        for k in ddata.keys():
            if sum(ddata[k]) == 0:
                del ddata[k]

        C = CampaignSum(campaign = c, data=json.dumps(ddata))
        C.save()

        for d in our_object[c]["hours"].keys():
            hdata = our_object[c]["hours"][d]

            if [sum(row) for row in hdata] == range(24): continue
            d = datetime.strptime(d, "%Y-%m-%d %H:%M:%S")

            D = DaySum(campaign = c, data=json.dumps(hdata), day=d)
            D.save()
    print "Data successfully ported to Django Models"
