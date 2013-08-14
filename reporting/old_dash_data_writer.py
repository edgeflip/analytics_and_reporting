#!/usr/bin/env python
# LOCAL VERSION
# SERVER VERSION NEEDS ABSOLUTE PATHS
# EXAMPLE:
# /home/ubuntu/reporting_app/current_data_va.csv
# /home/ubuntu/reporting_app/hourly_data_va.csv
from generate_report import generate_report_for_endpoint
from generate_report import make_hour_by_hour_object
from generate_report import make_day_by_day_object
import csv
import sys
import json

if __name__ == '__main__':
    # if we have passed a numerical client_id
    # we need to also generate our data object
    if sys.argv[1].isdigit():
        today, aggregate = generate_report_for_endpoint(sys.argv[1])
        hour_by_hour = make_hour_by_hour_object(sys.argv[1])
        day_by_day = make_day_by_day_object(sys.argv[1])
        if sys.argv[1] == '2':
            writer = csv.writer(open('current_data_va.csv','wb'), delimiter=',')
            writer1 = csv.writer(open('hourly_data_va.csv','wb'), delimiter=',')
            for row in hour_by_hour['data']:
                writer1.writerow(row)
                writer2 = csv.writer(open('daily_data_va.csv','wb'), delimiter=',')
            for row in day_by_day['data']:
                writer2.writerow(row)

            del writer1
            del writer2
            print "New data written"
        elif sys.argv[1] == '3':
            writer = csv.writer(open('current_data_anon.csv','wb'), delimiter=',')

        writer.writerow(today[0])
        writer.writerow(aggregate[0])
        del writer
    else:
        print "We need an integer as an argument"

