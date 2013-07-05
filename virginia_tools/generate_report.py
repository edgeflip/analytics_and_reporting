#!/usr/bin/env python
from time import strftime
from generate_data_for_export3 import tool
import datetime
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText	


# number of users shown queries
shown_not_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='button_load' AND (campaign_id='{0}' AND updated > FROM_UNIXTIME({1}));"
shown_aggregate = "SELECT COUNT(session_id) FROM events WHERE type='button_load' AND campaign_id='{0}';"

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
		percent_of_shared_clicked = round(float(num_click_total/float(num_shared_total), 2))
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
	
