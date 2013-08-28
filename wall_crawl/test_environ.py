#!/usr/bin/env python
from navigate_db import PySql as pysql

f = open('dbcreds.txt', 'r')
d = f.read().split('\n')
f.close()
orm = pysql(d[0], d[1], d[2], d[3])
orm.connect()
