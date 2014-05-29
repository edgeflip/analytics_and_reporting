#!/usr/bin/env python
import psycopg2
import sys
import MySQLdb as mysql
from boto.s3.connection import S3Connection
import csv, os, time
import logging
from keys import aws, rds, redshift
from redshift_utils import deploy_table, drop_table_if_exists


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
    l = 20000
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
    s3conn = S3Connection(aws['aws_access_key_id'], aws['aws_secret_access_key'])
    red = s3conn.get_bucket('redshiftxfer') 

    k = red.new_key(table)
    k.set_contents_from_filename('%s.csv' % table)
    logging.info("Uploaded %s to s3" % table)


def copy_from_s3(connection, cursor, table, staging_table, access_key, secret_key):
    with connection:
        cursor.execute(
            """
            COPY {0} FROM 's3://redshiftxfer/{1}'
            CREDENTIALS 'aws_access_key_id={2};aws_secret_access_key={3}' delimiter '|'
            """.format(staging_table, table, access_key, secret_key)
        )


def main(table):
    start = time.time()

    # get the schema of the table
    columns = None
    csvtime = None
    runcsv = None
    try:
        with mysql.connect( **rds) as dbconn:
            dbconn.execute("describe %s" % table)
            description = dbconn.fetchall()
            columns = create_query(description)
            write2csv(table, dbconn)
            csvtime = time.time()
            runcsv = csvtime - start
    except StandardError as e:
        logger.warning("error: {0}".format(e))

    redconn = psycopg2.connect( **redshift)
    redcursor = redconn.cursor()

    up2s3(table)
    ups3time = time.time()
    runs3 = ups3time - csvtime

    # create the table with the columns query now generated
    staging_table = "{0}_staging".format(table)
    old_table = "{0}_old".format(table)
    drop_table_if_exists(staging_table, redconn, redcursor)

    logging.info('Creating table {}'.format(staging_table))
    with redconn:
        redcursor.execute("CREATE TABLE {0} ({1})".format(staging_table, columns))

    # copy the file that we just uploaded to s3 to redshift
    access_key = aws['aws_access_key_id']
    secret_key = aws['aws_secret_access_key']
    try:
        copy_from_s3(redconn, redcursor, table, staging_table, access_key, secret_key)
    except psycopg2.DatabaseError:
        # step through the csv we are about to copy over and change the encodings to work properly with redshift
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
        copy_from_s3(redconn, redcursor, table, staging_table, access_key, secret_key)

    copytime = time.time()
    runcopy = copytime - ups3time

    deploy_table(table, staging_table, old_table, redcursor, redconn)

    endtime = time.time()
    runswap = endtime - copytime
    runtotal = endtime - start
    logging.info("Successfully copied %s from RDS to S3 to Redshift" % table)

    events = [
        ('write csv', runcsv),
        ('write to s3', runs3),
        ('copy from s3 to redshift', runcopy),
        ('swap redshift tables', runswap),
        ('complete entire process', runtotal)
    ]
    logging.info('|'.join("{0:.2f} seconds to {1}".format(duration, event) for event, duration in events))


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
 
