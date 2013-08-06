#!/usr/bin/env python

def connect_s3():
    from boto.s3.connection import S3Connection
    conn = S3Connection('AKIAJDIWDVVGWXFOSPEQ', 'RpcwFl6tw2XtOqnwbhXK9PemhUQ8kK6UdCMJ5GaI')
    return conn
