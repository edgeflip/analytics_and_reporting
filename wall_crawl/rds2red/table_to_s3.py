#!/usr/bin/env python
import sys
import MySQLdb as mysql
from con_s3 import connect_s3
import csv
import time


data_swap = {""}

copy = "COPY {0} FROM s3://{1}/{2}' CREDENTIALS 'aws_access_key_id={3};aws_secret_access_key={4}';"


"""
    The program takes as parameter database credentials and the name of the table we want
    to copy and names the output csv file the name of the table provided and copies that file to s3
"""

if __name__ == '__main__':
    # python this_program.py -h dbname -u username -p password -t table
    if len(sys.argv) != 11:
        #print len(sys.argv)
        print "This program takes arguments in the form\n -h hostname -u username -p password -d dbname -t table"
    else:
        host = sys.argv[2]
        user = sys.argv[4]
        password = sys.argv[6]
        dbname = sys.argv[8]
        table = sys.argv[10]
        conn = mysql.connect(host, user, password, dbname)
        cur = conn.cursor()
        # we need a load balancer for our cursor so that we don't run out of memory
        l = 10000
        o = 0
        start = time.time()
        cur.execute("select * from {0} limit {1} offset {2}".format(table, l, o))

        with open('%s.csv' % table, 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter='|')
            writer.writerows(cur)
            rows = cur.fetchall()
            while len(rows) > 0:
                o += l
                cur.execute("select * from {0} limit {1} offset {2}".format(table, l, o))
                writer.writerows(cur)
                rows = cur.fetchall()
        #end_time = time.time() - start
        #print "Table {0} written to csv\nWritten in increments of {1} in {2} seconds\n".format(table, str(l), str(end_time)) 
        conn = connect_s3()
        red = conn.get_bucket('redxfer')
        k = red.new_key()
        k.key = table
        k.set_contents_from_filename('%s.csv' % table)
        end_time = time.time() - start
        print "Table {0} moved from RDS to S3\nWritten in increments of {1} to S3 in {2} seconds".format(table, str(l), str(end_time))
        
