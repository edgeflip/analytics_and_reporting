#!/usr/bin/env python
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from time import strftime
from con_s3 import connect_s3

def mail_to_s3():
	to_send = ['wes@edgeflip.com','wesley7879@gmail.com']
	m = strftime('%m')
	d = str(int(strftime('%d'))-1)
	y = strftime('%Y')

	files = ['events_file_{0}_{1}_{2}.csv'.format(m,d,y),'ref_table_{0}_{1}_{2}.csv'.format(m,d,y)]

	msg = MIMEMultipart()
	msg['From'] = 'wes@edgeflip.com.com'
	mailserver = smtplib.SMTP('smtp.live.com',587)
	mailserver.ehlo()
	mailserver.starttls()
	mailserver.ehlo()
	mailserver.login('wes@edgeflip.com','gipetto3')
	
	for person in to_send:
		msg['To'] = person
		for i in range(len(files)):
			msg['Subject'] = 'S3 file %s uploaded' % files[i]
			filename = files[i]
			try:
				f = open(files[i],'r').read()
				if i == 0:
					message = 'https://s3.amazonaws.com/virginia_bucket/events_file_{0}_{1}_{2}'.format(m,d,y)
				else:
					message = 'https://s3.amazonaws.com/virginia_bucket/ref_table_{0}_{1}_{2}'.format(m,d,y)
				msg.attach(MIMEText(message))
				mailserver.sendmail('wesleymadrigal_99@hotmail.com',person,msg.as_string())
				print "%s sent to %s" % (files[i], person)
			except IOError:
				pass


