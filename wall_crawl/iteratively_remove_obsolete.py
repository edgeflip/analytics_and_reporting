#!/usr/bin/env python
from boto.s3.connection import S3Connection
import json

if __name__ == '__main__':
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    tokens = conn.get_bucket('fbtokens')
    no_data = [i for i in tokens.get_all_keys() if 'data' not in json.loads(i.get_contents_as_string()).keys()]
    while len(no_data) > 0:
        map(lambda x: tokens.delete_key(x), no_data)
        no_data = [i for i in tokens.get_all_keys() if 'data' no in json.loads(i.get_contents_as_string()).keys()]
    print "It is finished!"


