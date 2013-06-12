#!/usr/bin/env python
import MySQLdb as mysql



# pass a new PySql object a cursor for instantiation

class PySql(object):
	def __init__(self, cur):
		self._cur = cur
		
	def show_tables(self):
		try:
			self._cur.execute("SHOW TABLES;")
			# un-tuple the tables into a nicely parsable list
			tables = [i[0] for i in self._cur.fetchall()]
			return tables
		except:
			print "Error fetching the requested tables."

	def show_columns(self,table):
		try:
			self._cur.execute("SHOW COLUMNS IN %s" % table)
			columns = [i[0] for i in self._cur.fetchall()]
			return columns
		except:
			print "There is no table called %s" % table

	def query(self,query_string):
		try:
			self._cur.execute(query_string)
			res = self._cur.fetchall()
			res_list = [[e for e in i] for i in res]
			return res_list	
		except:
			print "You have a syntactical error in your SQL"

	"""
	json_ify takes as input a table name in the format of a string and a 
	SQL query string " SELECT * FROM users ... "
	json_ify simply calls previously defined methods and iterates through the data they yield
	create a new data structure for passing to the client
	
	"""

	# query all events in the campaign
	# SELECT * FROM events WHERE campaign_id = 3;

	# query pertinent information from users about the primary
	# SELECT email, gender, birthday, city, state FROM users WHERE fbid=fbid_from_campaign_query

	 

	def json_ify(self,table,query_string):
		data_object = {}
		res = self.query(query_string)
		keys = self.show_columns(table)
		data_object["data"] = {}
		for i in range(len(res)):
			data_object["data"][str(i)] = {}
			for e in range(len(keys)):
				data_object["data"][str(i)][keys[e]] = res[i][e]
		return data_object	


	# helper function for datetime stuff
	def convert_string_to_datetime(string):
		_format = '%Y-%m-%d %H:%M%S'
		result_datetime = datetime.strptime(string,_format)
		return result_datetime  

		
