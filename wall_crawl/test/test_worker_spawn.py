#!/usr/bin/env python
import sys
import os

if __name__ == '__main__':
    n = int(sys.argv[1])
    if n > 1:
        for i in range(n):
            cwd = os.getcwd()
            os.system("python {0}/worker.py {1}".format(cwd, str(n)))


