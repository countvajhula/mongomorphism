from pymongo import MongoClient

class Session(object):
	""" Holds database info, and whether the session is to be transactional or not (default yes)
	"""
	def __init__(self, dbname, host=None, port=None, transactional=True):
		self.connection = MongoClient(host, port)
		self.db = self.connection[dbname]
		self.transactional = transactional
