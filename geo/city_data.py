#!/usr/bin/env python
import urllib2
from math import *
from con_s3 import connect_s3
import json
from test_environ import orm_staging
import time

"""
   The primary tools being used here are 'retrieve()' and 'find_closest()'
   'gather()' is used to mine the data from our raw data file.
  
   To utilize the tools for proximity purposes we can open up a python shell and type the following

   >>> from city_data import retrieve, find_closest
   >>> # find the 10 closest cities to chicago
   >>> data = retrieve()
   >>> find_closest(data, 'Chicago', 'IL')
   ([(u'Berwyn', 9.053423164435735), (u'Hometown', 12.499052709887364), (u'Burbank', 12.596501215325173), (u'Hickory Hills', 17.433727307148832), (u'Countryside', 17.496747624287323), (u'Palos Hills', 19.492409913700545), (u'Blue Island', 20.00049763723764), (u'Northlake', 20.375285718969007), (u'Palos Heights', 21.54046584706006), (u'Elmhurst', 22.619524433223418)])
   
   The numbers next to our cities are the distance in km from our parameterized city

"""




"""
   gathers all of the data we want on state, city, and lat,lon coordinates and puts 
   into a hash table in the form {state: {city: {'type': 'city', 'coords': (lat, lon)}, city: {'type': 'town', 'coords': (lat, lon)}, city: {'type': 'village', 'coords': (lat, lon)}}}
   this is then turned into json and stored in s3 where it is read from and utilized by 
   the python implementation of haversine to find spherical distances
"""

def gather(ret = False):
    f = open('Gaz_places_national.txt', 'r').read()
    f = f.split('\t')
    n = len(f)/13
    # put all of the states as keys into data
    data = { each: {} for each in list(set([ [j.split('\n') for j in f[13+(13*x)].split('\r')][1][1] for x in range(n) ])) }
    for x in range(n-1):
        city_stuff = f[16+(13*x)].decode('latin-1').encode('utf-8').split(' ')
        _type = city_stuff[-1]
        city_stuff.pop()
        city = ' '.join(city_stuff)
        lat = ''.join([e for e in f[25+(13*x)] if e.isspace() != True])
        long_state = [j.split('\n') for j in f[26+(13*x)].split('\r')]
        _long = ''.join([e for e in long_state[0][0] if e.isspace() != True])
        state = long_state[1][1]
        data[state][city] = {"type": _type, "coords": (float(lat), float(_long))}
    conn = connect_s3()
    geo = conn.get_bucket('geoloc')
    k = geo.new_key()
    k.key = 'geo'
    k.set_contents_from_string(json.dumps(data))
    if ret:
        return data   
    else:
        print "Data added to s3" 



# retrieves the state, city, geocoord data from s3

def retrieve():
   conn = connect_s3()
   geo = conn.get_bucket('geoloc')
   k = geo.get_key('geo')
   res = k.get_contents_as_string()
   return json.loads(res)


# haversine is a geometric function derived from the law of haversines and it calculates 
# the curvature distance of the two cities using radians and the haversine formulae
# and then converts the distance into km by multiplying by the radius of the earth

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
    _all = [i for i in data[state].keys() if i != city and data[state][i]['type'] == 'city']
    closest = []
    lat1, lon1 = data[state][city]['coords'][0], data[state][city]['coords'][1]
    for each in _all:
        lat2, lon2 = data[state][each]['coords'][0], data[state][each]['coords'][1]
        distance = haversine(lat1, lon1, lat2, lon2)
        closest.append((each, distance))
    closest = sort_cities(closest)
    if number != None:
        return closest[0:number]
    else:
        return closest[0:10]


def euclidean(lat1, lon1, lat2, lon2):
    R = 6367
    distance = R*sqrt( (lat2 - lat1)**2 + cos( (lon2 - lon1)**2)**2 )
    return distance
