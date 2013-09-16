#!/usr/bin/env python
from crawl_tools import always_crawl_from_database
from test_environ import orm
from con_s3 import connect_s3

if __name__ == '__main__':
    conn = connect_s3()
    timestamp = conn.get_bucket('fbcrawl1').get_key('timestamp').get_contents_as_string()
    always_crawl_from_database(orm, timestamp)

