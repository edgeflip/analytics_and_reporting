#!/usr/bin/env python
from crawl_tools import always_crawl_from_database
from crawl_tools import crawl_realtime_updates
from crawl_tools import get_tokens_for_user
import time, os
from con_s3 import connect_s3
from test_environ import orm

if __name__ == '__main__':
	
	_time = str(int(time.time()))
	try:
            timestamp = connect_s3().get_bucket('fbcrawl1').get_key('timestamp').get_contents_as_string()
            always_crawl_from_database(orm, crawl_timestamp=timestamp)
	except AttributeError:
            always_crawl_from_database(orm)
                 
	crawl_realtime_updates(orm)
        conn = connect_s3()
        conn.get_bucket('fbcrawl1').get_key('timestamp').set_contents_from_string(_time)
        k = conn.get_bucket('fbcrawl1').new_key()
        k.key = 'endcrawl'
        k.set_contents_from_string(str(int(time.time())))
        os.system("python /home/ubuntu/crawl_stuff/build_weighted.py")
