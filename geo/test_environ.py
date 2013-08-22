#!/usr/bin/env python
from navigate_db import PySql as pysql


orm_prod = pysql('edgeflip-production-a-read1.cwvoczji8mgi.us-east-1.rds.amazonaws.com', 'root', 'YUUB2ctgkn8zfe', 'edgeflip')
orm_prod.connect()

orm_staging = pysql('edgeflip-db.efstaging.com', 'root', '9uDTlOqFmTURJcb', 'edgeflip')
orm_staging.connect()
