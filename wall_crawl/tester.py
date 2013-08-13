#!/usr/bin/env python
from test_environ import orm
from crawl_tools import crawl_realtime_updates2
from crawl_tools import always_crawl_from_database
import sys

sys.dont_write_bytecode = True

def tester():
	return crawl_realtime_updates2(orm)

def tester1():
	return always_crawl_from_database(orm)
