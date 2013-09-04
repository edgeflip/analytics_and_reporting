#!/usr/bin/env python
from sqlalchemy import create_engine
import psycopg2
from con_s3 import connect_s3
import sys
import MySQLdb as mysql
import csv

def create_conn():
   f = open("redcreds.txt", "r")
   d = f.read().split('\n')
   f.close()
   conn = psycopg2.connect(host=d[0], database=d[1], port=d[2], user=d[1], password=d[3])
   conn.autocommit = True
   return conn


def _map(val):
    redshift_vals = ['integer', 'bigint', 'decimal', 'real', 'double precision', 'boolean', 'char', 'varchar', 'date', 'timestamp']
    try:
        starts = val[0:3]
    except IndexError:
        starts = val[0:2]
    result = [ i for i in redshift_vals if i.startswith(starts) ]
    return result[0]


def create_query(d):
    q = ','.join([ ' '.join([ each[0], _map(each[1]) ]) for each in d ])
    return q 



if __name__ == '__main__':
    if len(sys.argv) == 2:
        table = sys.argv[1]
        dbcreds = open('dbcreds.txt', 'r').read().split('\n')
        dbconn = mysql.connect(dbcreds[0], dbcreds[1], dbcreds[2], dbcreds[3])
        cur = dbconn.cursor()  
        l = 10000
        o = 0
        # get the description
        cur.execute("describe %s" % table)
        description = cur.fetchall()

        cur.execute("select count(*) from %s" % table)
        n = cur.fetchall()
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

        s3conn = connect_s3()
        red = s3conn.get_bucket('redxfer')
        k = red.new_key()
        k.key = table
        k.set_contents_from_filename('%s.csv' % table)
        print "Uploaded to s3"
        
        # connect to redshift and copy the file that we just uploaded to s3 to redshift
        engine = create_engine('postgresql+psycopg2://', creator=create_conn)
        redconn = engine.connect()
        columns = create_query(description)
        # create the table with the columns query now generated
        redconn.execute("create table {0}({1})".format(table, columns))
        s3creds = open('s3creds.txt', 'r').read().split('\n')
        access_key = s3creds[0]
        secret_key = s3creds[1]
        redconn.execute("COPY {0} FROM 's3://redxfer/{0}' CREDENTIALS 'aws_access_key_id={1};aws_secret_access_key={2}' delimiter '|'".format(table, access_key, secret_key))
        print "Successfully copied %s from RDS to S3 to Redshift" % table

    else:
        print "Supply a table argument"

 
