from logging import debug, info
import cStringIO
import csv
from boto.s3.connection import S3Connection

def mkCSV(application, t=False, client_id=2):
    """ Grab event data for the hour preceding t """
    rs = application.pcur  # redshift cursor, need better naming convention

    # ok.. if i put this in the actual query, Redshift says it's not supported
    rs.execute("SELECT DATE_TRUNC('hour', now()) - INTERVAL '1 hour'")
    hour = rs.fetchone()[0]
    info('Gathering datapoints for client {}, hour {}'.format(client_id, str(hour)))


    # S3 connection, base filename
    from keys import aws
    S3 = S3Connection( **aws)
    bucket = S3.get_bucket('ef-client-data')
    basefile = "{}-{}-{}-{}-{}".format('virginia',hour.year,hour.month,hour.day,hour.hour)


    # EVENTS
    # get campaign_id, activity_id
    rs.execute( """    
    SELECT e.event_datetime AS time, v.session_id, v.fbid, e.friend_fbid, e.type, root.root_id, e.activity_id
    FROM events AS e, visits AS v, campchain as root 
        WHERE e.visit_id=v.visit_id 
        AND DATE_TRUNC('hour', time) = %s
        AND e.campaign_id IN
        (SELECT DISTINCT(campaign_id) FROM campaigns WHERE client_id=%s)
        AND e.type IN ('session_start', 'authorized', 'shared', 'clickback') 
        AND root.parent_id IS NOT NULL AND e.campaign_id=root.parent_id
    ORDER BY time DESC 
    """, (hour,client_id))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['time', 'session_id', 'fbid', 'friend_fbid','type', 'campaign_id', 'activity_id']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        debug(row)
        writer.writerow(row)

    # put it on S3
    f.seek(0)
    key = bucket.new_key(basefile + '-events.csv')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()  # clear memory

    # USERS
    rs.execute( """
    SELECT fbid,fname,lname,gender,city,state,birthday,email
    FROM users
    WHERE fbid IN
        (SELECT DISTINCT(fbid) FROM visits, events
        WHERE visits.visit_id=events.visit_id
            AND date_trunc('hour', events.event_datetime) = %s
            AND fbid IS NOT NULL
            AND events.campaign_id IN
            (SELECT DISTINCT(campaign_id) FROM campaigns WHERE client_id=%s)
        )
    """, (hour,client_id))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['fbid', 'fname', 'lname', 'gender','city','state','birthday','email']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        writer.writerow(row)

    # put it on S3
    f.seek(0)
    key = bucket.new_key(basefile + '-users.csv')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()

    # Params for Virginia's service
    import requests
    import json
    out = {'bucket':'ef-client-data'}
    out['type'] = 'EventFile'
    out['key'] = basefile+'-events.csv'

    headers = {'Content-Type': 'application/json'}

    requests.post('http://va-c2v.herokuapp.com/datafiles', data=json.dumps(out), headers=headers)
    out['type'] = 'UserFile'
    out['key'] = basefile+'-users.csv'
    requests.post('http://va-c2v.herokuapp.com/datafiles', data=json.dumps(out), headers=headers)

    import email
    msg = email.Message.Message()
    msg['Subject'] = 'Uploaded {} for VA'.format(basefile)

    import smtplib
    smtp = smtplib.SMTP()
    smtp.connect()
    smtp.sendmail('japhy@edgeflip.com', ['japhy@edgeflip.com',], msg.as_string())


def mkemailCSV(application, client_id=2):
    info('Making email CSV for client {}'.format(client_id))

    rs = application.pcur  # redshift cursor, need better naming convention

    # S3 connection, base filename
    from keys import aws
    S3 = S3Connection( **aws)
    bucket = S3.get_bucket('ef-client-data')

    rs.execute("""
        SELECT DISTINCT fname,lname,email 
        FROM users,visits,events,campaigns 
        WHERE users.fbid=visits.fbid 
            AND visits.visit_id=events.visit_id 
            AND events.campaign_id=campaigns.campaign_id 
            AND campaigns.client_id=%s
        """,(client_id,))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['fname','lname','email']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        debug(row)
        writer.writerow(row)


    # put it on S3
    f.seek(0)
    key = bucket.new_key('54e946104e477c00df6fd684e0955d7e')
    key.set_contents_from_file(f)
    key.set_acl('public-read')
    debug(f.getvalue())
    f.close()

    info('Done.')

def mkemailsummary(application, client_id=2):
    rs.execute("""
    SELECT
        root_id,
        campaigns.name,
        source,
        SUM(CASE WHEN t.type='button_load' THEN 1 ELSE 0 END) AS visits,
        SUM(CASE WHEN t.type='button_click' THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN t.type='authorized' THEN 1 ELSE 0 END) AS auths,
        COUNT(DISTINCT CASE WHEN t.type='authorized' THEN fbid ELSE NULL END) AS uniq_auths,
        COUNT(DISTINCT CASE WHEN t.type='shown' THEN fbid ELSE NULL END) AS shown,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN fbid ELSE NULL END) AS shares,
        COUNT(DISTINCT CASE WHEN t.type='shared' THEN t.friend_fbid ELSE NULL END) AS audience,
        COUNT(DISTINCT CASE WHEN t.type='clickback' THEN t.cb_visit_id ELSE NULL END) AS clickbacks
    FROM
        (
            SELECT e1.visit_id,
                e1.campaign_id,
                e1.content_id,
                e1.friend_fbid,
                e1.type,
                e1.content,
                e1.activity_id,
                NULL AS cb_visit_id,
                e1.updated
            FROM events e1
                WHERE type <> 'clickback'
            UNION
            (
            SELECT e3.visit_id,
                e3.campaign_id,
                e2.content_id,
                e3.friend_fbid,
                e2.type,
                e2.content,
                e2.activity_id,
                e2.visit_id AS cb_visit_id,
                e2.updated
            FROM events e2
            LEFT JOIN events e3 USING (activity_id)
                WHERE e2.type='clickback' AND e3.type='shared'
            )
        ) t
    INNER JOIN (SELECT fbid, source, visit_id FROM visits) v
        USING (visit_id)
    INNER JOIN campchain ON parent_id=campaign_id
    INNER JOIN campaigns ON campchain.root_id=campaigns.campaign_id
    WHERE campaigns.client_id=2
    GROUP BY root_id, source, campaigns.name
    ORDER BY root_id DESC
    """, (client_id,))

