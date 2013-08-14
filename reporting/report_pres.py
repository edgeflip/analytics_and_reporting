#!/usr/bin/env python
import os

"""
Builds the directories we need for our report data and writes the directive file
that will be used by the daemon and the reporting app to know where to read and
write from
"""

def build_pres():
    dir1, dir2 = "dash_data1", "dash_data2"
    cur_files = os.listdir(os.getcwd())
    if dir1 not in cur_files and dir2 not in cur_files:
	os.mkdir(dir1)
	os.mkdir(dir2)
	f = open("write_to.txt","w")
	f.write(dir1)
	f.close()
    else:
	pass 



if __name__ == '__main__':
    build_pres()
    
