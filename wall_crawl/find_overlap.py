#!usr/bin/env python
from navigate_db import PySql
import MySQLdb as mysql

def find_overlap():
	conn = mysql.connect('edgeflip-db.efstaging.com','root','9uDTl0qFmTURJcb','edgeflip')
	cur = con.cursor()

	orm = PySql(cur)
	# get every single connection and build the structure out of it
	_all = orm.query("SELECT fbid,ownerid FROM tokens")

	# some dict comprehensions
	struct = { x : [] for x,y in _all }

	{struct[x].append(y) for x,y in _all}

	originals = [i for i in struct.keys()]
	originals += [i for i in e if i not in originals for e in struct.values()]

	# get the total number of users accounting for overlap with no repeats
	total_non_repeat = len(originals)

	# get the length of all keys and their respective values not accounting for overlap
	total = len([e for e in struct.keys()] + [j for j in i for i in struct.values()])

	# return the percentage of total that is overlap 
	return 1.0 - float(total_non_repeat)/float(total)


if __name__ == '__main__':
	print find_overlap()
