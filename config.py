from pymongo import MongoClient
import transaction
import datamanager

class Session(object):
	""" Holds database info, and whether the session is to be transactional or not (default yes)
	A single session object can be shared by multiple participants in a transaction.
	"""
	def __init__(self, dbname, host=None, port=None, transactional=True):
		self.connection = MongoClient(host, port)
		self.db = self.connection[dbname]
		self.transactional = transactional
		self.transactionInitialized = False
		self.queue = []
		self.initialize()

	def initialize(self):
		""" On subsequent transactions after the initial one, a session can simply be reinitialized
		instead of a new instance being created.
		"""
		if self.transactional and not self.transactionInitialized:
			txn = transaction.get()
			txn.addBeforeCommitHook(datamanager.mongoListener_prehook, args=(), kws={})
			txn.addAfterCommitHook(datamanager.mongoListener_posthook, args=(), kws={'session':self})
			self.transactionInitialized = True
