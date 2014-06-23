from db_utils import *
import os
import urllib2
import httplib
import socket
import nltk
import sys
import re
from cookielib import CookieJar

def get_canonical_link(link):
    # after ? is important for youtube, but probably not other sites (mobile vs. regular)
    if 'youtube' not in link:
        link = link.strip().split('?')[0]
    if link and link[-1] == '/':
        link = link[:-1]
    return link

def get_filename_for_link(link):
    '''
    for file name, convert forward slash to underscore, remove colon, 
    and take first 200 characters
    '''    
    filename = re.sub("[//]", "_", link)
    filename = re.sub("/", "_", filename)
    filename = re.sub(":", "", filename)
    return filename[:200]

def get_all_links(conn):
    query = """
            SELECT link
            FROM fbid_sample_50000_links
            """
    rows = execute_query(query, conn)
    for row in rows:
        yield row[0]

def create_link_text_file(link, data_dir):    
    link_document_filename = data_dir + '/' + get_filename_for_link(link)
    if os.path.isfile(link_document_filename):
        return 0

    cj = CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj)) # for 303 redirects
    opener.addheaders = [('User-agent', 'Mozilla/5.0')] # for 404 that aren't actually not found if from a browser
    try:
        html = opener.open(link, timeout=10).read()
    except (urllib2.HTTPError, urllib2.URLError, httplib.BadStatusLine, httplib.IncompleteRead, socket.error) as e:
        sys.stdout.write('\tgerror: {}\n'.format(e))
        return -1

    text = nltk.clean_html(html)
    text = re.sub('\n', '', text)
    
    file = open(link_document_filename, 'w')
    file.write(text)
    file.close()
    return 1

if __name__ == '__main__':
    ## Parameters ##
    sample_size = 50000
    data_dir = '/data/user_links'
    broken_link_file_name = '/data/caches/broken_links.txt'

    conn = redshift_connect()

    broken_links = set()
    broken_link_file = open(broken_link_file_name, 'r')
    for line in broken_link_file:
        line = line.strip()
        broken_links.add(line)
    broken_link_file.close()

    broken_link_file = open(broken_link_file_name, 'a')
    for link in get_all_links(conn):
        link = get_canonical_link(link)
        if not link:
            continue
        if link in broken_links:
            sys.stdout.write('\tskipping {}\n'.format(link))
            continue
        sys.stdout.write('{}\n'.format(link))
        sys.stdout.flush()
        created = create_link_text_file(link, data_dir)
        if created == 0:
            sys.stdout.write('\talready exists\n')
        elif created == -1:
            if link not in broken_links:
                broken_links.add(link)
                broken_link_file.write('{}\n'.format(link))
                broken_link_file.flush()

    broken_link_file.close()
    redshift_disconnect(conn)