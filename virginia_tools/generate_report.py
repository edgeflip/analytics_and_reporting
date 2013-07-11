#!/usr/bin/env python
from time import strftime
from generate_data_for_export_original import tool
import datetime
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText	
import csv
from Crypto.Cipher import DES
import logging.config
import urllib
import base64
import time

# visitors, authorizations, shown friends, shared, # visitors shared with, clickbacks


# base query, mutable for visitors, authorizations, shown friends, share

baseline_query = "SELECT COUNT(session_id) FROM events WHERE (type='{0}' AND content_id='{1}') AND (campaign_id='{2}' AND updated > FROM_UNIXTIME({3}));"

visitors_shared_with_query = "SELECT COUNT(session_id) FROM events WHERE (type='shared' AND campaign_id='{0}' AND content_id='{1}' AND updated > FROM_UNIXTIME({2})) AND friend_fbid IN (SELECT fbid FROM events WHERE type='button_load');"

campaign_stuff = "SELECT name FROM campaigns WHERE campaign_id='{0}'"
content_stuff = "SELECT name FROM client_content WHERE content_id='{0}'"

# encryption stuff for our campaign_id/content_id DES encryption

secret = '5un W@h!'
cipher = DES.new(secret)
PADDING = ' '
BLOCK_SIZE = 8
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE)*PADDING

def encodeDES(message):
    """Encrypt a message with DES cipher, returning a URL-safe, quoted string"""
    message = str(message)
    encrypted = cipher.encrypt(pad(message))
    b64encoded = base64.urlsafe_b64encode(encrypted)
    encoded = urllib.quote(b64encoded)
    return encoded


def generate_report2(campaign_id, content_id, timestamp):
	
	# get all campaign and content pertinent data for today and aggregate by formatted the queries from above
	visitors_today = tool.query(baseline_query.format('button_load',campaign_id, content_id,timestamp))[0][0]
	visitors_aggregate = tool.query(baseline_query.format('button_load',campaign_id, content_id,0))[0][0]
	
	auths_today = tool.query(baseline_query.format('authorization',campaign_id,content_id,timestamp))[0][0]
	auths_aggregate = tool.query(baseline_query.format('authorization',campaign_id,content_id,0))[0][0]

	shown_today = tool.query(baseline_query.format('shown',campaign_id,content_id,timestamp))[0][0]
	shown_aggregate = tool.query(baseline_query.format('shown',campaign_id,content_id,0))[0][0]

	shared_today = tool.query(baseline_query.format('shared',campaign_id,content_id,timestamp))[0][0]
	shared_aggregate = tool.query(baseline_query.format('shared',campaign_id,content_id,0))[0][0]

	visitors_shared_with_today = tool.query(visitors_shared_with_query.format(campaign_id,content_id,timestamp))[0][0]
	visitors_shared_with_aggregate = tool.query(visitors_shared_with_query.format(campaign_id,content_id,0))[0][0]

	clickback_today = tool.query(baseline_query.format('clickback',campaign_id,content_id,timestamp))[0][0]
	clickback_aggregate = tool.query(baseline_query.format('clickback',campaign_id,content_id,0))[0][0]

	campaign_name = tool.query(campaign_stuff.format(campaign_id))[0][0]
	content_name = tool.query(content_stuff.format(content_id))[0][0]

	# encrypt our campaign_id and content_id with the encodeDES algorithm
	des_message = encodeDES(str(campaign_id) + '/' + str(content_id))

	m = strftime('%m')
	d = strftime('%d')
	y = strftime('%Y')

	f = open('virginia_report_{0}_{1}_{2}.csv'.format(m,d,y),'wb')
	writer = csv.writer(f,delimiter=',')
	writer.writerow(['%s Targeted Sharing Report' % campaign_name])
	writer.writerow(['Campaigns currently running'])
	writer.writerow(['Campaign id, Campaign name'])
	writer.writerow([campaign_id, campaign_name])
	writer.writerow(['Content currently running'])
	writer.writerow(['Content id, Content name'])
	writer.writerow([content_id, content_name])
	writer.writerow(['Stats for today ({0}/{1}/{2})'.format(m,d,y)])
	writer.writerow(['Campaign/content/slug','visitors', 'authorizations', '# people shown friends', '# friends shared with', '# visitors shared with', '# clickbacks'])
	writer.writerow([des_message, visitors_today, auths_today, shown_today, shared_today, visitors_shared_with_today, clickback_today])
	writer.writerow(['Stats from beginning to now'])
	writer.writerow(['Campaign/content/slug', 'visitors','authorizations', '# people shown friends', '# friends shared with', '# visitors shared with', '# clickbacks'])
	writer.writerow([des_message, visitors_aggregate, auths_aggregate, shown_aggregate, shared_aggregate, visitors_shared_with_aggregate, clickback_aggregate])
	
	f.close()
	print "Report for campaign_id %s and content_id %s generated" % (str(campaign_id), str(content_id))



# queries for master report
baseline_query_master = "SELECT COUNT(session_id) FROM events WHERE type='{0}' AND updated > FROM_UNIXTIME({1});"
visitors_shared_with_master = "SELECT COUNT(session_id) FROM events WHERE (type='shared' AND updated > FROM_UNIXTIME({0})) AND friend_fbid IN (SELECT fbid FROM events WHERE type='button_load');"

def generate_master_report(timestamp):
	m = strftime('%m')
	d = strftime('%d')
	y = strftime('%Y')
	
	visitors_today = tool.query(baseline_query_master.format('button_load',timestamp))[0][0]
	visitors_total = tool.query(baseline_query_master.format('button_laod',0))[0][0]
	
	auths_today = tool.query(baseline_query_master.format('authorization',timestamp))[0][0]
	auths_total = tool.query(baseline_query_master.format('authorization',0))[0][0]
	
	shown_today = tool.query(baseline_query_master.format('shown',timestamp))[0][0]
	shown_total = tool.query(baseline_query_master.format('shown',0))[0][0]
	
	shared_today = tool.query(baseline_query_master.format('shared',timestamp))[0][0]
	shared_total = tool.query(baseline_query_master.format('shared',0))[0][0]
	
	visitors_shared_today = tool.query(visitors_shared_with_master.format(timestamp))[0][0]
	visitors_shared_total = tool.query(visitors_shared_with_master.format(0))[0][0]

	clickback_today = tool.query(baseline_query_master.format('clickback',timestamp))[0][0]
	clickback_total = tool.query(baseline_query_master.format('clickback',0))[0][0]

	# get the amount of days that we have been in operation divide numbers by that amount
	day_started = tool.query("SELECT MIN(updated) FROM events;")[0][0]
	days_str = str(datetime.datetime.now() - day_started)
	end_of_days = days_str.find('days')-1
	# the number of days the campaign has been running
	days = int(days_str[0:end_of_days])
	try:	
		avg_visitors_daily = round(float(visitors_total)/float(days),1)
	except ZeroDivisionError:
		avg_visitors_daily = 0
	try:
		avg_auths_daily = round(float(auths_total)/float(days),1)
	except ZeroDivisionError:
		avg_auths_daily = 0
	try:
		auth_rate_choose_visitors = round(float(auths_total)/float(visitors_total),2)
	except ZeroDivisionError:
		auth_rate_choose_visitors = 0.0
	try:
		avg_shown_daily = round(float(shown_total)/float(days),1)
	except ZeroDivisionError:
		avg_shown_daily = 0
	try:
		avg_shared_daily = round(float(shared_total)/float(days),1)
	except ZeroDivisionError:
		avg_shared_daily = 0
	try:
		share_rate_choose_shown = round(float(shared_total)/float(shown_total),2)
	except ZeroDivisionError:
		share_rate_choose_shown = 0.0
	try:
		avg_clickback_daily = round(float(clickback_total)/float(days),1)
	except ZeroDivisionError:
		avg_clickback_daily = 0
	try:
		clickback_rate_choose_shared = round(float(clickback_total)/float(shared_total),2)
	except ZeroDivisionError:
		clickback_rate_choose_shared = 0.0
	
	
	with open('master_report_{0}_{1}_{2}.csv'.format(m,d,y),'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=',')
		writer.writerow(['visitors today', 'auths today', 'shown today', 'shared today', 'visitors shared today', 'clickbacks today'])
		writer.writerow([visitors_today,auths_today,shown_today,shared_today,visitors_shared_today,clickback_today])
		writer.writerow(['visitors total', 'auths total', 'shown total', 'shared total', 'visitors shared total', 'clickbacks total'])
		writer.writerow([visitors_total,auths_total,shown_total,shared_total,visitors_shared_total,clickback_total])
		writer.writerow(['avg visitors', 'avg auths'])
		writer.writerow([avg_visitors_daily,avg_auths_daily])
		writer.writerow(['auth rate per visitor'])
		writer.writerow([auth_rate_choose_visitors])
		writer.writerow(['avg shown', 'avg shared'])
		writer.writerow([avg_shown_daily,avg_shared_daily])
		writer.writerow(['share rate per shown'])
		writer.writerow([share_rate_choose_shown])
		writer.writerow(['avg clickback'])
		writer.writerow([avg_clickback_daily])
		writer.writerow(['clickback rate per share'])
		writer.writerow([clickback_rate_choose_shared])
	print "Master report for %s generated" % m+'-'+d+'-'+y

############################################################################################################################################

# ORIGINAL REPORT GENERATION QUERIES



# number of users shown queries
visitors_not_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='button_load' AND (campaign_id='{0}' AND updated > FROM_UNIXTIME({1}));"
visitors_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='button_load' AND campaign_id='{0}';"

# number of authorized queries
auth_not_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='authorized' AND (campaign_id='{0}' AND updated > FROM_UNIXTIME({1}));"
auth_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='authorized' AND campaign_id='{0}';"

# number of shared queries
shared_not_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='shared' AND (campaign_id='{0}' AND updated > FROM_UNIXTIME({1}));"
shared_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='shared' AND campaign_id='{0}';"

# number of clickbacks queries
click_not_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='clickback' AND (campaign_id='{0}' AND updated > FROM_UNIXTIME({1}));"
click_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='clickback' AND campaign_id='{0}';"

# number of shown queries
shown_not_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='shown' AND (campaign_id='{0}' AND updated > FROM_UNIXTIME({1}));"
shown_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='shown' AND campaign_id='{0}'"

# campaign start time
start_time = "SELECT MIN(updated) FROM events WHERE campaign_id='{0}';"
	



def generate_daily_report(campaign_id, timestamp):
	num_visitors_total = tool.query(shown_aggregate.format(campaign_id))[0][0]
	num_visitors_today = tool.query(shown_not_aggregate.format(campaign_id,timestamp))[0][0]
	num_auths_total = tool.query(auth_aggregate.format(campaign_id))[0][0]
	num_auths_today = tool.query(auth_not_aggregate.format(campaign_id,timestamp))[0][0]
	num_shared_total = tool.query(shared_aggregate.format(campaign_id))[0][0]
	num_shared_today = tool.query(shared_not_aggregate.format(campaign_id,timestamp))[0][0]
	num_click_total = tool.query(click_aggregate.format(campaign_id))[0][0]
	num_click_today = tool.query(click_not_aggregate.format(campaign_id,timestamp))[0][0]
	num_shown_total = tool.query(shown_aggregate.format(campaign_id))[0][0]
	num_shown_today = tool.query(shown_not_aggregate.format(campaign_id,timestamp))[0][0]	
	auth_rate = round(float(int(num_auths_total))/float(int(num_visitors_total)), 2)
	day_started = tool.query(start_time.format(campaign_id))[0][0]
	days_str = str(datetime.datetime.now() - day_started)
	end_of_days = days_str.find('days')-1
	# the number of days the campaign has been running
	days = int(days_str[0:end_of_days])
	average_friends_shown_per_day = round(float(num_shown_total)/float(days), 2)
	average_friends_shared_per_day = round(float(num_shared_total)/float(days), 2)
	percent_of_shown_shared = round(float(num_shared_total)/float(num_shown_total), 2)
	try:
		average_clickbacks = round(float(num_click_total)/float(days), 2)
	except ZeroDivisionError:
		average_clickbacks = 0.0
	try:
		percent_of_shown_clicked = round(float(num_click_total)/float(num_shown_total), 2)
	except ZeroDivisionError:
		percent_of_shown_clicked = 0.0
	try:
		percent_of_shared_clicked = round(float(num_click_total)/float(num_shared_total), 2)
	except ZeroDivisionError:
		percent_of_shared_clicked = 0.0

	m = strftime('%m')
	d = strftime('%d')
	y = strftime('%Y')
	today = m+'-'+d+'-'+y
	report = open("report_{0}_{1}_{2}.txt".format(m,d,y), "w")
	
	report.write("{0} visitors on {1}\n".format(str(num_visitors_today), today))
	report.write("{0} visitors total\n\n".format(str(num_visitors_total)))
	report.write("{0} authorizations on {1}\n".format(str(num_auths_today), today))
	report.write("{0} authorizations total\n".format(str(num_auths_total)))
	report.write("\t{0} authorization rate\n\n".format(str(auth_rate)))
	report.write("{0} shown per day\n".format(str(average_friends_shown_per_day)))
	report.write("{0} shared per day\n".format(str(average_friends_shared_per_day)))
	report.write("{0} clickback per day\n".format(str(average_clickbacks)))
	report.write("\t{0} of shown have clickbacks\n".format(str(percent_of_shown_clicked)))
	report.write("\n{0} of shared have clickbacks\n".format(str(percent_of_shared_clicked)))
	report.close()

	print "Report for %s generated" % today



def email_report(campaign_id):
        creds = open('.credentials.txt','r').read().split('\n')
        creds.pop()
        m, d, y = strftime('%m'), strftime('%d'), strftime('%Y')
        todays_report = 'report_{0}_{1}_{2}.txt'.format(m,d,y)
        msg = MIMEMultipart()
        f = file(todays_report)
        attachment = MIMEText(f.read())
        msg['From'] = creds[0]
        msg['Subject'] = 'Report for %s' % m+'-'+d+'-'+y
        attachment.add_header('Content-Disposition', 'attachment', filename=todays_report)
        msg.attach(attachment)
        mailserver = smtplib.SMTP('smtp.gmail.com',587)
        mailserver.ehlo()
        mailserver.starttls()
        mailserver.ehlo()
        mailserver.login(creds[0],creds[1])
        people = ['wes@edgeflip.com','rayid@edgeflip.com','kit@edgeflip.com','matt@edgeflip.com','mark@edgeflip.com','john@edgeflip.com']
        for person in people:
                mailserver.sendmail(creds[0],person,msg.as_string())



def generate_report_tests(campaign_id, timestamp=None):
	if timestamp:
		shown_query = shown_not_aggregate.format(campaign_id, timestamp)
		num_shown = tool.query(shown_query)

		auth_query = auth_not_aggregate.format(campaign_id, timestamp)
		num_auth = tool.query(auth_query)

		shared_query = shared_not_aggregate.format(campaign_id, timestamp)
		num_shared = tool.query(shared_query)

		click_query = click_not_aggregate.format(campaign_id, timestamp)
		num_clicked = tool.query(click_query)

		return num_shown, num_auth, num_shared, num_clicked

	else:
		shown_query = shown_aggregate.format(campaign_id)
		num_shown = tool.query(shown_query)

		auth_query = auth_aggregate.format(campaign_id)
		num_auth = tool.query(auth_query)

		shared_query = shared_aggregate.format(campaign_id)
		num_shared = tool.query(shared_query)

		click_query = click_aggregate.format(campaign_id)
		num_clicked = tool.query(click_query)

		return num_shown, num_auth, num_shared, num_clicked
	
