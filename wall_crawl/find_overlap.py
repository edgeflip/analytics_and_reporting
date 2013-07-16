#!usr/bin/env python
from navigate_db import PySql
import MySQLdb as mysql

def find_overlap():
    conn = mysql.connect('edgeflip-db.efstaging.com','root','9uDTlOqFmTURJcb','edgeflip')
    cur = conn.cursor()

    orm = PySql(cur)
    # get every single connection and build the structure out of it
    _all = orm.query("SELECT fbid,ownerid FROM tokens")

    # some dict comprehensions
    keys = list(set([x for x,y in _all]))
    struct = { x : [] for x in keys }

    {struct[x].append(y) for x,y in _all}

    _all_listed = [e for e in struct[x] for x in keys]
    _all_listed_with_repeats = keys + _all_listed

    length_with_repeats = len(_all_listed_with_repeats)

    # add each key once
    no_repeats = [key for key in keys]
    # add all elements accounting for repeats and not adding them
    for key in struct.keys():
        for element in struct[key]:
            if element not in no_repeats:
                no_repeats.append(element)
    no_repeats_length = len(no_repeats)

    return 1.0 - (float(no_repeats_length)/float(length_with_repeats))

if __name__ == '__main__':
    print find_overlap()
