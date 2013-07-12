#!/usr/bin/env python
from crawl_tools import always_crawl_from_database
from crawl_tools import crawl_realtime_updates
from crawl_tools import get_tokens_for_user
import time
from navigate_db import PySql

if __name__ == '__main__':
	orm = PySql('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
    	orm.connect()

	try:
		timestamp = open('crawl_stamp.txt','r').read()
		always_crawl_from_database(orm, crawl_timestamp=timestamp)
	except IOError:
		always_crawl_from_database(orm)
	crawl_realtime_updates(orm)
	f = open('crawl_stamp.txt','w')
	f.write(str(int(time.time())))
	f.close()
