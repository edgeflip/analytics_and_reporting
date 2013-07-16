#!/usr/bin/env python
import sys
import requests
import time

"""
    Using the requests module we will run a series of 
    user-specified tests against our endpoint that we 
    have established to display the reports.
"""

if __name__ == '__main__':
    amount = sys.argv[1]
    url = sys.argv[2]
    if amount.isdigit():
        start_time = time.time()
        n = 0
        for i in range(int(amount)):
            r = requests.get(url)
            if r.status_code == 200:
                n += 1
            elif r.status_code != 200:
                end_time = time.time()
                total_time = end_time-start_time
                print "Too much load\n{0} requests in {1} seconds\n".format(str(n),str(int(total_time)))
                break
            else:
                print "%s,%s" % (r.status_code,r.reason)
                break
        end_time = time.time()
        total_time = int(end_time-start_time)
        print "\n{0} GET requests ran successfully in {1} seconds\n".format(str(n), str(total_time))
    else:
        print "We need an integer"


