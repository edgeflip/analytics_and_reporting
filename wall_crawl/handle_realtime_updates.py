#!/usr/bin/env python
from flask import Flask
from crawl_tools import run_crawler, crawl_feed, subscribe_user

app = Flask(__name__)

@app.route('/endpoint_for_updates',methods=['GET','POST'])
def realtime_updates():
	if request.method == 'POST':
		run_crawler()
		
