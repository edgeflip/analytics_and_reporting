#!/usr/bin/env python
from time import strftime


"""

	We need this module to count how many people authorized the campaign today, and of those who authorized how many shared, and how many clickbacks we had

"""


def generate_basic():
	auths  = open("authorization_data.csv", "r")
	auths = auths.read().split('\n')
	auths = [i.split(',') for i in auths]
	auths.pop()
	
	total_without_trues = len(auths)
	total_auths = len([i for i in auths if i[3] == '1\r'])

	shared = open("share_data.csv", "r")
	shared = shared.read().split('\n')
	shared = [i.split(',') for i in shared]
	shared.pop()

	total_share_attempts = len(shared)
	total_successful_shares = len([e for e in shared if e[4] == '1\r'])

	clickback = open("clickback_data.csv","r")
	clickback = clickback.read().split('\n')
	
	total_clickbacks = len(clickback)

	# percentages
	try: 
		success_auth_percent = 100*(float(total_auths)/float(total_without_trues))
	except ZeroDivisionError:
		success_auth_percent = "no results"
	try:
		share_percentage_of_total_attempt_shared = 100 * (float(total_successful_shares)/float(total_share_attempts))
	except ZeroDivisionError:
		share_percentage_of_total_attempt_shared = "no results"
	try:
		share_percentage_of_auths = 100 * (float(total_successful_shares)/float(total_auths))
	except ZeroDivisionError:
		share_percentage_of_auths = "no results"
	
	try:
		clickback_portion_of_auths = 100 * (float(total_clickbacks)/float(total_auths))
	except ZeroDivisionError:
		clickback_portion_of_auths = "no results"
	try:
		clickback_portion_of_shares = 100 * (float(total_clickbacks)/float(total_successful_shares))
	except ZeroDivisionError:
		clickback_portion_of_shares = "no results"

	m = strftime("%m")
	d = strftime("%d")
	report = open("report_{0}_{1}.txt".format(m,d), "w")

	# auth stuff
	report.write("{0} authorization attempts\n{1} authorization successes\n".format(str(total_without_trues),str(total_auths)))
	report.write("\t{0}% authorization success\n\n".format(success_auth_percent))

	# share stuff
	report.write("{0} share attempts\n{1} share successes\n".format(str(total_share_attempts),str(total_successful_shares)))
	report.write("\t{0}% of share attempts were successfully shared\n".format(str(share_percentage_of_total_attempt_shared)))
	report.write("\t{0}% of authorizations were successfully shared\n\n".format(str(share_percentage_of_auths)))

	# clickback stuff
	report.write("{0} clickbacks\n".format(str(total_clickbacks)))
	report.write("\t{0}% of successful shares were clicked back\n".format(str(clickback_portion_of_shares)))
	report.write("\t{0}% of successful authorizations were clicked back\n\n".format(str(clickback_portion_of_auths)))

	report.close()

	print "Report Generated"



	
	
