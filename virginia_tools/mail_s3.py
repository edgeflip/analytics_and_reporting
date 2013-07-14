#!/usr/bin/env python
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from time import strftime

def mail_to_s3():
	to_send = ['rayid@edgeflip.com','wes@edgeflip.com','wesley7879@gmail.com']
	m = strftime('%m')
	d = str(int(strftime('%d'))-1)
	y = strftime('%Y')

	files = ['events_file_{0}_{1}_{2}.csv'.format(m,d,y),'ref_table_{0}_{1}_{2}.csv'.format(m,d,y)]

	for person in to_send:
		for _file in files:
			msg = MIMEMultipart()
			msg['From'] = 'wesleymadrigal_99@hotmail.com'
			msg['To'] = person
			msg['Subject'] = 'S3 file %s uploaded' % _file
			filename = _file
			f = open(_file,'r').read()
			attachment = MIMEText(f)
			attachment.add_header('Content-Disposition','attachment',filename=filename)
			msg.attach(attachment)
			mailserver = smtplib.SMTP('smtp.live.com',587)
			mailserver.ehlo()
			mailserver.starttls()
			mailserver.ehlo()
			mailserver.login('wesleymadrigal_99@hotmail.com','madman2890')
			mailserver.sendmail('wesleymadrigal_99@hotmail.com',person,msg.as_string())
			print "%s sent to %s" % (_file, person)


