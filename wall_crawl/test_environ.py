#!/usr/bin/env python
from navigate_db import PySql as pysql


orm = pysql('edgeflip-production-a-read1.cwvoczji8mgi.us-east-1.rds.amazonaws.com', 'root', 'YUUB2ctgkn8zfe', 'edgeflip')
orm.connect()
