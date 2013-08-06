#!/usr/bin/env python
# object structure for crawler
from crawl_metrics import metrics
import json
import urllib2

class Person(dict):
    def __init__(self, fbid):
        self.fbid = self['fbid'] = fbid
        self.edges = self['edges'] = []
        self.all_ids = []

    def add_edge(self, edge):
        self.edges.append(edge)
                                                    
    def get_all_post_ids(self):
        for edge in self.edges:
            self.all_ids += edge.post_ids
                                                                    
                                                                    
    # for a given edge there is a token, a feed, and metrics around the aforementioned
class Edge(dict):
    def __init__(self, fbid, ownerid, token):
        self.fbid = self['fbid'] = fbid
        self.ownerid = self['ownerid'] = ownerid
        self.token = self['token'] = token
        self.graph_api = self['graph_api'] = 'https://graph.facebook.com/{0}?fields=feed.since({1})&access_token={2}'
        # list of [post['id'] for post in blob['feed']['data']]
        # we will use this to avoid duplicate posts for a user
        # this attribute will be called at a higher level to generate
        # an even more comprehensive list of post_ids
        self.post_ids = self['post_ids'] = []
        self.feed = self['feed'] = None

    def crawl_feed(self, since):
        formatted = self.graph_api.format(self.fbid, since, self.token)
        blob = json.loads(urllib2.urlopen(formatted).read())
        try:
            these_ids = [(blob['feed']['data'].index(each), each['id']) for each in blob['feed']['data']]
            for index, _id in these_ids:
                # if we have this id in our current ids
                # delete it from our blob
                if _id in self.post_ids:
                    del blob[index]
            self.feed = blob
        except KeyError:
            pass

        try:
            self.feed = blob['feed']['data'] + self.feed['feed']['data']
        except KeyError, TypeError:
            pass


                                                                                                                
    def get_connectedness(self):
        import json
        import urllib2
        from crawl_metrics import metrics
        try:
            self._metrics = self['metrics'] = metrics(self.feed)
        except AttributeError:
            formatted_api = self.graph_api.format(self.fbid, 0, self.token)
            result = json.loads(urllib2.urlopen(formatted_api).read())
            _metric = metrics(result) 
            self._metrics = metric
            try:
                self._feed
            except AttributeError:
                self._feed = result
