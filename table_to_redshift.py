#!/usr/bin/env python
import psycopg2
import sys
import MySQLdb as mysql
from boto.s3.connection import S3Connection
import csv, os, time
import logging


def connect_s3():
    f = open('creds/s3creds.txt', 'r')
    f1 = f.read().split('\n')
    f.close()
    first = f1[0]
    second = f1[1]
    conn = S3Connection(first, second)
    return conn


def create_conn():
    from keys import redshift
    return psycopg2.connect( **redshift)


def _map(val):
    #redshift_vals = ['integer', 'bigint', 'decimal', 'real', 'double precision', 'boolean', 'char', 'varchar', 'date', 'timestamp']
    redshift_vals = {
        'int' : 'integer', 
        'mediumint': 'bigint',
        'tinyint': 'integer',
        'bigint' : 'bigint', 
        'decimal' : 'decimal', 
        'real' : 'real', 
        'double precision' : 'double precision', 
        'boolean' : 'boolean', 
        'char' : 'char(50)',  # ehhh.. this is usually ips
        'varchar' : 'varchar(1028)', 
        'date' : 'date', 
        'timestamp': 'timestamp',
        'datetime': 'timestamp',  # kinda magic
        'longtext': 'varchar',  # .. really magic
        }


    out = redshift_vals[val.split('(')[0]]
    return out


def create_query(d):
    q = ','.join([ ' '.join([ each[0], _map(each[1]) ]) for each in d ])
    return q 


def write2csv(table, cur):
    logging.debug('Creating CSV for {}'.format(table))
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
    k = red.new_key(table)
    k.set_contents_from_filename('%s.csv' % table)
    logging.info("Uploaded %s to s3" % table)


def main(table, redconn=None):
    start = time.time()
    dbcreds = open('creds/dbcreds.txt', 'r').read().split('\n')
    dbconn = mysql.connect(dbcreds[0], dbcreds[1], dbcreds[2], dbcreds[3])
    cur = dbconn.cursor()  

    # get the schema of the table
    cur.execute("describe %s" % table)
    description = cur.fetchall()
    columns = create_query(description)

    redshiftconn = create_conn()
    redconn = redshiftconn.cursor()

    # connect to redshift and copy the file that we just uploaded to s3 to redshift
    try:
        logging.debug('Creating table {}'.format(table))
        redconn.execute("CREATE TABLE _{0} ({1})".format(table, columns))
    except Exception as e:
        redshiftconn.rollback()
        logging.debug(e.pgerror)  # basically, "table already exists"
        # redconn.execute("DROP TABLE %s" % table)
        redconn.execute("CREATE TABLE _{0} ({1})".format(table, columns))
        time.sleep(1)
    
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
        redconn.execute("COPY _{0} FROM 's3://redxfer/{0}' CREDENTIALS 'aws_access_key_id={1};aws_secret_access_key={2}' delimiter '|'".format(table, access_key, secret_key))
    except:
        # redshiftconn.commit()  # eh but really we want to rollback and redo the CREATE TABLE

        # step through the csv we are about to copy over and change the encodings to work propely with redshift
        logging.info("Error copying, assuming encoding errors and rewriting CSV...")
 
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
 
        logging.info("Rewrite complete")
        os.remove('%s.csv' % table)
        os.system("mv {0}2.csv {0}.csv".format(table))
        up2s3(table)
        # atomicity insurance
        time.sleep(10)
        redconn.execute("COPY _{0} FROM 's3://redxfer/{0}' CREDENTIALS 'aws_access_key_id={1};aws_secret_access_key={2}' delimiter '|'".format(table, access_key, secret_key))
 
    redshiftconn.commit()
   
    redtime = int( time.time() )
    redrun = redtime - ups3time
    totalrun = redtime - start       
    logging.info( "Successfully copied %s from RDS to S3 to Redshift" % table)
    logging.info( "{0} seconds to write csv | {1} seconds to write to s3 | {2} seconds to copy from s3 to redshift | {3} seconds to complete entire process".format( str(runcsv), str(runs3), str(redrun), str(totalrun) ))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Supply a table argument"
    else:
        import sys
        
        root = logging.getLogger()

        # eh, uncomment to get logging to stdout
        root.setLevel(logging.DEBUG)
        
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)

        table = sys.argv[1]
        main(table)
 
