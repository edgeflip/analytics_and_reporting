#!/usr/bin/env python

import sys
import logging

from edgeflip import database as db
from edgeflip.settings import config

logger = logging.getLogger(__name__)

conn = db.getConn()
curs = conn.cursor()


def auths_and_reach(clientId):

    results = {}

    sql = "SELECT COUNT(*) AS total_auths FROM user_clients WHERE client_id = %s"
    curs.execute(sql, clientId)
    results['total_auths'] = curs.fetchone()[0]
    logger.info("Client %s has %d total authorizations" % (clientId, results['total_auths']))

    sql = """SELECT COUNT(*) AS current_auths FROM user_clients uc
            JOIN clients c USING(client_id)
            JOIN tokens t ON c.fb_app_id = t.appid AND uc.fbid = t.fbid
            WHERE t.fbid = t.ownerid AND t.expires > CURRENT_TIMESTAMP
            AND uc.client_id = %s
        """
    curs.execute(sql, clientId)
    results['current_auths'] = curs.fetchone()[0]
    logger.info("Client %s has %d current authorizations" % (clientId, results['current_auths']))

    sql = """SELECT COUNT(*) AS total_edges, COUNT(DISTINCT fbid) AS total_reach, 
            SUM(CASE WHEN t.expires > CURRENT_TIMESTAMP THEN 1 ELSE 0 END) AS current_edges,
            COUNT(DISTINCT CASE WHEN t.expires > CURRENT_TIMESTAMP THEN fbid ELSE NULL END) AS current_reach
            FROM user_clients uc
            JOIN clients c USING(client_id)
            JOIN tokens t c.fb_app_id = t.appid AND uc.fbid = t.ownerid
            WHERE uc.client_id = %s"""
    curs.execute(sql, clientId)
    results['total_edges'], results['total_reach'], reslults['current_edges'], results['current_reach'] = curs.fetchone()
    logger.info("Client %s can reach %s people through %s edges" % (clientId, results['total_reach'], results['total_edges']))
    logger.info("Client %s can currently reach %s people through %s edges" % (clientId, results['current_reach'], results['current_edges']))

    return results


def user_flow_events(clientId):

    results = {}
    sql = """SELECT e.campaign_id, e.content_id,
        SUM(CASE WHEN type='button_load' THEN 1 ELSE 0 END) AS button_loads,
        SUM(CASE WHEN type='button_click' THEN 1 ELSE 0 END) AS button_clicks,

        SUM(CASE WHEN type='authorized' THEN 1 ELSE 0 END) AS auths,
        SUM(CASE WHEN type='auth_fail' THEN 1 ELSE 0 END) AS auth_fails,

        SUM(CASE WHEN type='shown' THEN 1 ELSE 0 END) AS friends_shown,
        COUNT(DISTINCT CASE WHEN type='shown' THEN friend_fbid ELSE NULL END) AS distinct_shown,

        SUM(CASE WHEN type='share_click' THEN 1 ELSE 0 END) AS share_clicks,
        SUM(CASE WHEN type='share_fail' THEN 1 ELSE 0 END) AS share_fails,

        COUNT(DISTINCT CASE WHEN type='shared' THEN session_id ELSE NULL END) AS num_shares,
        COUNT(DISTINCT CASE WHEN type='shared' THEN fbid ELSE NULL END) AS distinct_sharers,
        SUM(CASE WHEN type='shared' THEN 1 ELSE 0 END) AS total_recips,
        COUNT(DISTINCT CASE WHEN type='shared' THEN friend_fbid ELSE NULL END) AS distinct_recips,

        SUM(CASE WHEN type='suppressed' THEN 1 ELSE 0 END) AS suppressions,

        # click rate: button_click / button_shown
        SUM(CASE WHEN type='button_click' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN type='button_load' THEN 1 ELSE 0 END), 0) AS click_rate,
        # authorization rate: authorized / button_click
        SUM(CASE WHEN type='authorized' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN type='button_click' THEN 1 ELSE 0 END), 0) AS auth_rate,
        # auth fail rate: auth_fail / button_click
        SUM(CASE WHEN type='auth_fail' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN type='button_click' THEN 1 ELSE 0 END), 0) AS auth_fail_rate,
        # share rate: num_shares / authorized
        COUNT(DISTINCT CASE WHEN type='shared' THEN session_id ELSE NULL END) / NULLIF(SUM(CASE WHEN type='authorized' THEN 1 ELSE 0 END), 0) AS share_rate,
        # recips per share: total_recips / num_shares
        SUM(CASE WHEN type='shared' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT CASE WHEN type='shared' THEN session_id ELSE NULL END), 0) AS recips_per_share,
        # share fail rate: share_fail / share_click
        SUM(CASE WHEN type='share_fail' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN type='share_click' THEN 1 ELSE 0 END), 0) AS share_fail_rate,
        # suppressions per share: suppressed / num_shares
        SUM(CASE WHEN type='suppressed' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT CASE WHEN type='shared' THEN session_id ELSE NULL END), 0) AS suppress_per_share,
        # suppress-to-recipient ratio: suppressed / total_recips
        SUM(CASE WHEN type='suppressed' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN type='shared' THEN 1 ELSE 0 END), 0) AS suppress_recip_ratio

    FROM events e JOIN campaigns c USING(campaign_id)
    WHERE c.client_id = %s
    GROUP BY 1,2
    """

    curs.execute(sql, clientId)

    for row in curs.fetchall():
        results[(row[0], row[1])] = {
            'button_loads' : row[2],
            'button_clicks' : row[3],
            'auths' : row[4],
            'auth_fails' : row[5],
            'friends_shown' : row[6],
            'distinct_shown' : row[7],
            'share_clicks' : row[8],
            'share_fails' : row[9],
            'num_shares' : row[10],
            'distinct_sharers' : row[11],
            'total_recips' : row[12],
            'distinct_recips' : row[13],
            'suppressions' : row[14],
            'click_rate' : row[15],
            'auth_rate' : row[16],
            'auth_fail_rate' : row[17],
            'share_rate' : row[18],
            'recips_per_share' : row[19],
            'share_fail_rate' : row[20],
            'suppress_per_share' : row[21],
            'suppress_recip_ratio' : row[22]
        }
        logger.info('Client %s user flow events for campaign %s with content %s:\n%s' % (clientId, row[0], row[1], str(results[(row[0], row[1])])))

    return results


def clickbacks(clientId):

    results = {}
    sql = """SELECT e.campaign_id, e.content_id,
        SUM(CASE WHEN type='clickback' THEN 1 ELSE 0 END) AS total_clickbacks,
        SUM(CASE WHEN type='clickback' AND activity_id IS NULL THEN 1 ELSE 0 END) AS null_activity_clickbacks
    FROM events e JOIN campaigns c USING(campaign_id)
    WHERE c.client_id = %s
    GROUP BY 1,2
    """
    curs.execute(sql, clientId)

    for row in curs.fetchall():
        results[(row[0], row[1])] = {
            'total_clickbacks' : row[2],
            'null_activity_clickbacks' : row[3]     # Hopefully this is zero!!
        }
        if (row[3]):
            logger.error('%s clickbacks with null activity id found for client %s with campaign %s and content %s' % (row[3], clientId, row[0], row[1]))


    # Huh... obviously can't just join the events table on itself since 
    # multiple share records have the same activity_id.

    sql = """CREATE LOCAL TEMPORARY TABLE recip_counts ( KEY(campaign_id, content_id activity_id) )
        SELECT e.campaign_id, content_id, activity_id, COUNT(*) AS num_recips
        FROM campaigns c JOIN events e USING(campaign_id)
        WHERE e.type='shared' AND e.activity_id IS NOT NULL
            AND c.client_id = %s
        GROUP BY 1,2,3
    """
    curs.execute(sql, clientId)


    sql = """CREATE LOCAL TEMPORARY TABLE clickback_counts ( KEY(campaign_id, content_id activity_id) )
        SELECT e.campaign_id, content_id, activity_id, COUNT(*) AS num_clickbacks
        FROM campaigns c JOIN events e USING(campaign_id)
        WHERE e.type='clickback' AND e.activity_id IS NOT NULL
            AND c.client_id = %s
        GROUP BY 1,2,3
    """
    curs.execute(sql, clientId)


    sql = """SELECT rc.campaign_id, rc.content_id,
        SUM(num_recips) AS total_recips,
        SUM(num_clickbacks) AS total_clickbacks,
        SUM(num_clicbacks) / NULLIF(SUM(num_recips), 0) AS clickback_rate
        FROM recip_counts rc LEFT JOIN clickback_counts cc USING(campaign_id, content_id, activity_id)
        GROUP BY 1,2
    """
    curs.execute(sql)

    for row in curs.fetchall():
        results[(row[0], row[1])]['clickback_rate'] : row[4]
        logger.info('Client %s had a clickback rate of %s for campaign %s with content %s' % (clientId, row[0], row[1], row[4]))

    return results



if (__name__ == '__main__'):
    clientId = sys.argv[1]
    auths_and_reach(clientId)
    user_flow_events(clientId)
    clickbacks(clientId)



