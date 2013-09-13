#!/usr/bin/env python
from crawl_tools import crawl_realtime_updates
from test_environ import orm

if __name__ == '__main__':
    crawl_realtime_updates(orm)
