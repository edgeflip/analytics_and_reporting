#!/usr/bin/env python
import datetime
import json
import os
import sys


if __name__ == '__main__':
        f = open("/home/wes/Documents/analytics_and_reporting/reporting/tests/test_data.txt", "w")
        this_time = str(datetime.datetime.now())
        the_time = {"data": this_time}
        f.write("%s" % json.dumps(the_time))
        f.close()
        print "Successful rewrite at {0}".format(this_time)
