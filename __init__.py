import transaction
import datamanager
from config import Session

transactionsInitialized = False
txn_inject_synch = None # synch handle needs to remain in runtime scope otherwise transaction synchronizer will not be called
transactionBegun = False # flag to check whether transaction.begin() was called. Currently this appears to be necessary to trigger the newTransaction() synch callback

class MongoTxnInject(object):
	def newTransaction(self, txn):
		""" At the start of a new transaction, this adds a "mongo listener hook" which
		checks if any mongodb documents are part of the current transaction at commit-time,
		in which case the necessary preparations are made in mongodb
		"""
		txn.addBeforeCommitHook(datamanager.mongoListener_prehook, args=(), kws={'transaction':txn})
		globals()['transactionBegun'] = True

	def beforeCompletion(self, txn):
		pass

	def afterCompletion(self, txn):
		globals()['transactionBegun'] = False


def initialize(dbname, host=None, port=None, transactional=True):
	""" Application needs to call this once before the transaction machinery can be used
	"""
	session = Session(dbname, host, port, transactional)
	if transactional and not globals()['transactionsInitialized']: # not quite "thread-safe" but GIL should take care of that
		globals()['txn_inject_synch'] = MongoTxnInject()
		transaction.manager.registerSynch(globals()['txn_inject_synch'])
		globals()['transactionsInitialized'] = True
	return session
