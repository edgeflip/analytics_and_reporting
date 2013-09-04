#!/usr/bin/env python
import datetime, time
import json
from time import strftime
from models import get_campaign_stuff_for_client
from models import tool

# should be able to just do
# from models import CampaignSum, DaySum and use them

# this will rely on Django models CampaignSum and DaySum being accessible

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
    return our_object
    
    for client in our_object.keys():
        for campaign in our_object[client].keys():
            ddata = our_object[client][campaign]["days"]
        #    for k in ddata.keys():
        #        if sum(ddata[k]) == 0:
        #            del ddata[k]

            C = CampaignSum( campaign=campaign, data=json.dumps(ddata) )
            C.save()

            for day in our_object[client][campaign]["hours"].keys():
                hdata = our_object[client][campaign]["hours"][day]

            # if [sum(row) for row in hdata] == range(24): continue
            # d = datetime.strptime(d, "%Y-%m-%d %H:%M:%S")

                D = DaySum( campaign=campaign, data=json.dumps(hdata), day=day )
                D.save()
    print "Data successfully ported to Django Models"



"""
    This will likely be the function that is called periodically via a cron after the initial
    data scrape of all our clients and campaigns respectively.  A sort of moving window of 
    30 days of data for all campaigns and clients
"""

# given the django models CampaignSum and DaySum.....
# CampaignSum.objects.all()
# DaySum.objects.all()

def keep_updated():
    import random
    this_month = create_unix_time_for_each_day()
    this_month = [ time.mktime(datetime.datetime(j.year, j.month, j.day).timetuple()) for j in [datetime.datetime.fromtimestamp(i) for i in this_month] ]
    cur_hour = strftime('%H')
    clients = tool.query("select client_id, name from clients")

    # this will be useful later on as well....    
    today = datetime.datetime.now()
    current_day_key = str( datetime.datetime( today.year, today.month, today.day ) )

    # get a random client and a random campaign from Django to look at
    rand_id, rand_name = clients[ random.randint(0, len(clients)) ]
    # get the campaigns for the randomly selected client and get the latest day/hr
    campaigns = get_campaign_stuff_for_client( rand_id )
    rand_camp = campaigns[ random.randint(0, len(campaigns)) ]
    qset1 = DaySum.objects.get( campaign=rand_camp )
    qset2 = CampaignSum.objects.get( campaign=rand_camp )
    latest_hour = None
    latest_day = None

    # try getting the latest hour of today
    try:
        data = qset.filter( day=current_day_key ).data
        data = json.loads(data)
        latest_hour = max([ h[0] for h in data[ current_day_key ] ])

    except:
        data = json.loads(qset2.data)     
        cur_month_stored = [ i for i in cur_data[random_camp]['days'].keys() ]
        cur_month_stored = [ time.strptime(i, "%Y-%m-%d %H:%M:%S") for i in cur_month_stored ]
        cur_month_stored = [ time.mktime(datetime.datetime(j.tm_year, j.tm_mon, j.tm_mday).timetuple()) for j in cur_month_stored ]
        new_times = [ i for i in this_month if i not in cur_month_stored ]
        latest_day = max(cur_month_stored)
        latest_day = str( datetime.datetime.fromtimestamp( latest_day ) )
        # if we have reached a new day and it isn't in the dataset...

    if len(new_times) == 1:
        # want to query the data
        current_hour = int(strftime('%H'))
        new_day = int(new_times[0])
        new_day_key = str( datetime.datetime.fromtimestamp( new_day ) )
        updated_data = tool.query( main_query_hour_by_hour_new.format( str(new_day) ) )
         
        # build out our data structure for each client and campaign
        for client_id, name in clients:
            this_client = get_campaign_stuff_for_client(client_id)
            # need a way to get this clients data so that can be updated
            for campaign_id, campaign_name in this_client:
                # build hour stuff out and then sum to build day
                # methods to add to Django models
                cur_day_data = json.loads( CampaignSum.objects.get( campaign = campaign_name ).data )
                cur_hour_data = json.loads( DaySum.objects.get( campaign = campaign_name ).data )
                
                hours = [ r[4:] for r in updated_data if r[0] == campaign_id ]
                hours = [ [int(j) for j in i] for i in hours ]
                # if the maximum hour we have here is bigger than current_hour replace current_hour
                if len(hours) > 0:
                    if max([ h[0] for h in hours ]) > current_hour:
                        current_hour = max([ h[0] for h in hours ])
                to_add = [ [y] + [ 0 for x in range(9) ] for y in range(current_hour) if y not in [ have[0] for have in hours ] ]
                hours = hours + to_add
                # add the data to our object and restore
                cur_hour_data[new_day_key] = hours

                H = DaySum( campaign=campaign_name, data=json.dumps(cur_hour_data), day=new_day_key)
                H.save()
                
                day = []
                for j in range(1, len(hours[0])):
                    cur_val = sum([each[j] for each in hours])
                    day.append(cur_val)    
                # add data to our object
                cur_day_data[new_day_key] = day
                # put back in the database 
                C = CampaignSum( campaign=campaign_name, data=json.dumps(cur_day_data) )
                C.save()
	        # updated the data and save
    else:
        # get the data from the newest hour forward
        timestamp = time.mktime(datetime.datetime(today.year, today.month, today.day, latest_hour).timetuple() )
        data = tool.query(main_query_hour_by_hour_new.format(timestamp))
        for client_id, client_name in clients:
            client_stuff = get_campaign_stuff_for_client(client_id)
            for campaign_id, campaign_name in client_stuff:
                cur_day_data = json.loads( CampaignSum.objects.get( campaign=campaign_name ).data )
                cur_hour_data = DaySum.objects.get( campaign=campaign_name )
                cur_hour_data = json.loads( cur_hour_data.filter( day=current_day ).data )
                
                hour_data = [ i[4:] for i in data if i[0] == campaign_id ]
                # convert from longs to ints
                hour_data = [ [int(j) for j in i] for i in hour_data ]
                hrs_have = [ h[0] for h in hour_data ]
                hrs_dont = [ h for h in range(latest_hour) if h not in hrs_have ]
                new = [ [y] + [0 for x in range(9)] for y in hrs_dont ]
                hour_data += new
                # update todays hour data
                cur_hour_data[ current_day_key ] = hour_data

                H = DaySum( campaign=campaign_name, data=json.loads(cur_hour_data), day=current_day_key )
                H.save()

                day_data = []
                for i in range(1,len(new[0])):
                    day_data.append( sum( [ each[i] for each in new ] ) )
                cur_day_data[ current_day_key ] = day_data

                D = CampaignSum( campaign=campaign_name, data=json.loads(cur_day_data) )
                D.save()
    print "Data Updated"               
                  
                

# SQL QUERIES

main_query_hour_by_hour_new ="""SELECT                                                         
         e4.campaign_id,
         YEAR(t.updated),
         MONTH(t.updated),
         DAY(t.updated),
         HOUR(t.updated),
         SUM(CASE WHEN t.type='button_load' THEN 1 ELSE 0 END) as Visits,       
         SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) as Clicks, 
         SUM(CASE WHEN t.type='authorized' THEN 1 ELSE 0 END) as Authorizations,
         COUNT(DISTINCT CASE WHEN t.type='authorized' THEN t.fbid ELSE NULL END) as "Distinct Facebook Users Authorized",
         COUNT(DISTINCT CASE WHEN t.type='shown' THEN t.fbid ELSE NULL END) as "# Users Shown Friends",
         COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.fbid ELSE NULL END) as "# Users Who Shared",
         SUM(CASE WHEN t.type='shared' THEN 1 ELSE 0 END) as "# Friends Shared with",
         COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) as "# Distinct Friends Shared",
         COUNT(DISTINCT CASE WHEN t.type='clickback' THEN t.cb_session_id ELSE NULL END) as "# Clickbacks"
     FROM                                                                       
         (SELECT e1.*,NULL as cb_session_id FROM events e1 WHERE type <> 'clickback'
         UNION                                                                  
         SELECT e3.session_id,e3.campaign_id, e2.content_id,e2.ip,e3.fbid,e3.friend_fbid,e2.type,e2.appid,e2.content,e2.activity_id, e2.session_id as cb_session_id,e2.updated FROM events e2 LEFT JOIN events e3 USING (activity_id)  WHERE e2.type='clickback' AND e3.type='shared')
     t                     
         LEFT JOIN (SELECT session_id,campaign_id FROM events WHERE type='button_load')
     e4                                                                         
         USING (session_id)
         WHERE t.updated > FROM_UNIXTIME({0}) 
         GROUP BY e4.campaign_id, YEAR(t.updated), MONTH(t.updated), DAY(t.updated), HOUR(t.updated);"""

def all_hour_query():
    month = month_ago()
    res = tool.query(main_query_hour_by_hour_new.format(month))
    return res


# HELPER FUNCTIONS

def month_ago():
    one_month = 30 * 24 * 60 * 60
    return str(int(time.time())-one_month)

def get_campaign_stuff_for_client(client_id):
    res = tool.query("select campaign_id, name from campaigns where client_id='{0}' and campaign_id in (select distinct campaign_id from events where type='button_load')".format(client_id))
    return res


def create_unix_time_for_each_day():
    start = int(month_ago())
    days = []
    for i in range(30):
        start += 86400
        days.append(start)
    return days


def make_fake():
    campaign = "Wes Madrigal for President"
    days = create_unix_time_for_each_day()
    days = [ datetime.datetime.fromtimestamp(j) for j in days ]
    days = [ str( datetime.datetime( x.year, x.month, x.day ) ) for x in days ]
    data = { campaign: { "days" : { day: [ random.randint(1,1000) for i in range(9) ] for day in days }, "hours": { day: [ [j] + [random.randint(1,10) for x in range(9)] for j in range(24) ] for day in days } } }
    return data
 
