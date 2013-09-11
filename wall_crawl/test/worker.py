#!/usr/bin/env python
import multiprocessing
from multiprocessing import Pool
import sys
import pdb

def foo(n):
    return n*n

if __name__ == '__main__':
    #multiprocessing.set_start_method("forkserver")
    pdb.set_trace()
    pool = Pool(processes=4)
  
    print pool.map(foo, range(10))

    for i in pool.imap_unordered(foo, range(10)):
        print i

    # evaluate f(10) asynchronously
    res = pool.apply_async(foo, [10])
    print(res.get(timeout=1))
