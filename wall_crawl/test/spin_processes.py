#!/usr/bin/env python
import flask
from flask import request
from flask import render_template
from multiprocessing import Pool
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import sys


#app = flask.Flask(__name__)


def send_email(message):
    msg = MIMEMultipart()
    msg['From'] = 'wes@edgeflip.com'
    msg['To'] = 'wesley7879@gmail.com'
    msg['Subject'] = 'some test shit'
    msg.attach(MIMEText(message))
    mailserver = smtplib.SMTP('smtp.gmail.com', 587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login('wes@edgeflip.com', 'gipetto3')
    mailserver.sendmail('wes@edgeflip.com', 'wesley7879@gmail.com', msg.as_string()) 
    print "Email sent"


if __name__ == '__main__':
    n = int(sys.argv[1])
    pool = Pool(processes=n)
    msg = 'this is a test message'
    pool.apply_async(send_email, ['message'])
