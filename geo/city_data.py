#!/usr/bin/env python
import urllib2
from math import *
from con_s3 import connect_s3
import json

# by measure of the length of our data set we can iterate through


"""
   gathers all of the data we want on state, city, and lat,lon coordinates and puts 
   into a hash table in the form {state: {city: (lat, lon), city: (lat, lon), city: (lat, lon)}, state: {city: (lat, lon) .....} } ...
   this is then turned into json and stored in s3 where it is read from and utilized by 
   the python implementation of haversine to find spherical distances
"""

def gather(ret = None):
    f = open('Gaz_places_national.txt', 'r').read()
    f = f.split('\t')
    n = len(f)/13
    # put all of the states as keys into data
    data = { each: {} for each in list(set([ [j.split('\n') for j in f[13+(13*x)].split('\r')][1][1] for x in range(n) ])) }
    for x in range(n-1):
        city = f[16+(13*x)].decode('latin-1').encode('utf-8')
        lat = ''.join([e for e in f[25+(13*x)] if e.isspace() != True])
        long_state = [j.split('\n') for j in f[26+(13*x)].split('\r')]
        _long = ''.join([e for e in long_state[0][0] if e.isspace() != True])
        state = long_state[1][1]
        data[state][city] = {}
        data[state][city] = (float(lat), float(_long))
    conn = connect_s3()
    geo = conn.get_bucket('geoloc')
    k = geo.new_key()
    k.key = 'geo'
    k.set_contents_from_string(json.dumps(data))
    if ret:
        return data   
    else:
        print "Data added to s3" 


"""
   retrieves the state, city, geocoord data from s3
"""

def retrieve():
   conn = connect_s3()
   geo = conn.get_bucket('geoloc')
   k = geo.get_key('geo')
   res = k.get_contents_as_string()
   return json.loads(res)


def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    hlat = sin(dlat/2)**2
    hlon = sin(dlon/2)**2
    a = hlat + cos(lat1) * cos(lat2) * hlon
    c = 2 * asin(sqrt(a))
    # earth's radius varies between 6356 to 6378 km so 6367 is the average of the two
    km = 6367 * c
    return km


def sort_cities(L):
    for i in range(len(L)):
        for j in range(len(L)-1):
            if L[j][1] > L[j+1][1]:
                tmp = L[j]
                L[j] = L[j+1]
                L[j+1] = tmp
    return L


def find_closest(data, city, state, number=None):
    _all = [i for i in data[state].keys() if i != city]
    closest = []
    lon1, lat1 = data[state][city][1], data[state][city][0]
    for each in _all:
        lon2, lat2 = data[state][each][1], data[state][each][0]
        distance = haversine(lat1, lon1, lat2, lon2)
        closest.append((each, distance))
    closest = sort_cities(closest)
    if number != None:
        return closest[0:number]
    else:
        return closest[0:10]
