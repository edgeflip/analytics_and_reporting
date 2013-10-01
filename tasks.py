from logging import debug, info
import cStringIO
import csv


def mkCSV(application, t=False, client_id=2):
    """ Grab event data for the hour preceding t """
    rs = application.pcur  # redshift cursor, need better naming convention

    # ok.. if i put this in the actual query, Redshift says it's not supported
    rs.execute("SELECT DATE_TRUNC('hour', now()) - INTERVAL '1 hour'")
    hour = rs.fetchone()[0]
    info('Gathering datapoints for client {}, hour {}'.format(client_id, str(hour)))

    # EVENTS
    rs.execute( """    
    SELECT e.event_datetime AS time, v.session_id, v.fbid, e.friend_fbid, e.type
    FROM events AS e,visits AS v 
        WHERE e.visit_id=v.visit_id 
        AND DATE_TRUNC('hour', time) = %s
        AND e.campaign_id IN
        (SELECT DISTINCT(campaign_id) FROM campaigns WHERE client_id=%s)
    ORDER BY time DESC 
    LIMIT 40;
    """, (hour,client_id))

    # then make some csvs
    f = cStringIO.StringIO()
    headers = ['time', 'session_id', 'fbid', 'friend_fbid','type']
    writer = csv.writer(f, delimiter=",")
    writer.writerow(headers)
    for row in rs.fetchall():
        writer.writerow(row)

    # and TODO: put it on S3

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

    debug(f.getvalue())
    f.close()

