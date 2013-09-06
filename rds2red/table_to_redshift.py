#!/usr/bin/env python
from sqlalchemy import create_engine
import psycopg2
from con_s3 import connect_s3
import sys
import MySQLdb as mysql
import csv, os, time

def create_conn():
   f = open("creds/redcreds.txt", "r")
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


def write2csv(table, cur):
    l = 10000
    o = 0 
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


def up2s3(table): 
    s3conn = connect_s3()
    red = s3conn.get_bucket('redxfer') 
    k = red.new_key()
    k.key = table
    k.set_contents_from_filename('%s.csv' % table)
    print "Uploaded %s to s3" % table        


if __name__ == '__main__':
    if len(sys.argv) == 2:
        start = time.time()
        table = sys.argv[1]

        dbcreds = open('creds/dbcreds.txt', 'r').read().split('\n')
        dbconn = mysql.connect(dbcreds[0], dbcreds[1], dbcreds[2], dbcreds[3])
        cur = dbconn.cursor()  
        # get the description
        cur.execute("describe %s" % table)
        description = cur.fetchall()
        # connect to redshift and copy the file that we just uploaded to s3 to redshift
        engine = create_engine('postgresql+psycopg2://', creator=create_conn)
        redconn = engine.connect()
        columns = create_query(description)
     
        try:
            redconn.execute("create table {0}({1})".format(table, columns))
        except:
            delete_current = raw_input("%s already exists\nDo you want to delete rewrite?: " % table)
            if delete_current.lower() == 'y' or delete_current.lower() == 'yes':
                redconn.execute("drop table %s" % table)
                time.sleep(1)
                redconn.execute("create table {0}({1})".format(table, columns))
            elif delete_current.lower() == 'n' or delete_current.lower() == 'no':
                print "Process aborted"
                exit()
        
        write2csv(table, cur)
         
        csvtime = int( time.time() )
        runcsv = csvtime - start

        up2s3(table)
        ups3time = int( time.time() )
        runs3 = ups3time - csvtime
 
        # create the table with the columns query now generated

        s3creds = open('creds/s3creds.txt', 'r').read().split('\n')
        access_key = s3creds[0]
        secret_key = s3creds[1]
        try:
            redconn.execute("COPY {0} FROM 's3://redxfer/{0}' CREDENTIALS 'aws_access_key_id={1};aws_secret_access_key={2}' delimiter '|'".format(table, access_key, secret_key))
        except:
           # step through the csv we are about to copy over and change the encodings to work propely with redshift
           print "Rewriting file...."
   
           with open('%s.csv' % table, 'r') as csvfile:
               reader = csv.reader(csvfile, delimiter='|')
               with open('%s2.csv' % table, 'wb') as csvfile2:
                   writer = csv.writer(csvfile2, delimiter='|')
                   keep_going = True
                   while keep_going:
                       try:
                           this = reader.next()
                           new = [ i.decode('latin-1').encode('utf-8') for i in this ]
                           writer.writerow(new)
                       except StopIteration:
                           keep_going = False

           print "Rewrite complete"
           os.remove('%s.csv' % table)
           os.system("mv {0}2.csv {0}.csv".format(table))
           up2s3(table)
           # atomicity insurance
           time.sleep(10)
           redconn.execute("COPY {0} FROM 's3://redxfer/{0}' CREDENTIALS 'aws_access_key_id={1};aws_secret_access_key={2}' delimiter '|'".format(table, access_key, secret_key))
       
        redtime = int( time.time() )
        redrun = redtime - ups3time
        totalrun = redtime - start       
        print "Successfully copied %s from RDS to S3 to Redshift\n\n" % table
        print "Metrics:\n\t{0} seconds to write csv\n\t{1} seconds to write to s3\n\t{2} seconds to copy from s3 to redshift\n\t{3} seconds to complete entire process".format( str(runcsv), str(runs3), str(redrun), str(totalrun) )

    else:
        print "Supply a table argument"

 
