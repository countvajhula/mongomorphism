from pymongo import MongoClient
import transaction
import hooks

class Session(object):
	""" Holds database info, and whether the session is to be
	transactional or not (default yes). A single session object
	can be shared by multiple participants in a transaction.
	"""
	def __init__(self, dbname, host=None, port=None, transactional=True):
		self.connection = MongoClient(host, port)
		self.db = self.connection[dbname]
		self.transactional = transactional
		self.active = False
		self.queue = []
		self.begin()

	def begin(self):
		""" On subsequent transactions after the initial one,
		a session can simply be begun again instead of a
		new instance being created.
		"""
		if self.transactional and not self.active:
			txn = transaction.get()
			txn.addBeforeCommitHook(hooks.mongo_listener_prehook,
			                        args=(), kws={})
			txn.addAfterCommitHook(hooks.mongo_listener_posthook,
			                       args=(), kws={'session':self})
			self.active = True

	def close(self):
		""" End a session. This should be called at the end of interaction
		with the db, to remove mongo-specific hooks from the transaction.
		The session can be begun again by calling begin().
		Note: If there are changes already queued in the current
		transaction, these will give an error if a commit is attempted;
		they should be aborted by using transaction.abort().
		"""
		self.active = False

