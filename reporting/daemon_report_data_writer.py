#!/usr/bin/env python
from generate_report import generate_report_for_endpoint
from generate_report import make_hour_by_hour_object
import csv
import sys
import json

if __name__ == '__main__':
    # if we have passed a numerical client_id
    # we need to aslso generate our data object
    if sys.argv[1].isdigit():
        today, aggregate = generate_report_for_endpoint(sys.argv[1])
        hour_by_hour = make_hour_by_hour_object(sys.argv[1])
        if sys.argv[1] == '2':
            writer = csv.writer(open('current_data_va.csv','wb'), delimiter=',')
            f = open('hourly_data_va.py','w')
            f.write("hourly_data = %s" % json.dumps(hour_by_hour))
            f.close()
        elif sys.argv[1] == '3':
            writer = csv.writer(open('current_data_anon.csv','wb'), delimiter=',')

        writer.writerow(today[0])
        writer.writerow(aggregate[0])
        del writer
    else:
        print "We need an integer as an argument"

