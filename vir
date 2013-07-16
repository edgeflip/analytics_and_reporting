#!/usr/bin/env python
import time
from time import strftime

def handle_time_difference():
	now = time.time()
        # subtract hours minutes and seconds from now and that is midnight
        server_hour = int(strftime('%H'))
        server_min = int(strftime('%M'))
        server_sec = int(strftime('%S'))
        hours_in_seconds = server_hour * 60 * 60
        minutes_in_seconds = server_min * 60
        midnight = now - hours_in_seconds - minutes_in_seconds - server_sec
        return midnight


