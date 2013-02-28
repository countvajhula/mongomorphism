from pymongo import MongoClient
import transaction
from transaction.interfaces import TransientError
import logging
from hashlib import sha256
import platform
import datetime

#
# transaction support stuff
#

def gen_transaction_id(transaction):
	""" Generate a globally unique id for a transaction """
	timestamp = str(datetime.datetime.utcnow()) # particular moment in time
	local_id = str(id(transaction)) # guaranteed to be unique on this machine (but not all machines concurrently acting) at the present moment
	host_id = platform.node() # hostname as something 'globally unique'
	# alternatively MAC address, not sure if reliable: from uuid import getnode; mac = getnode()
	global_id = sha256("|".join((timestamp, local_id, host_id))).hexdigest()
	return global_id # repeated calls for the same transaction would NOT return the same ID - should be called only to generate a unique ID

def mongoListener_prehook(*args, **kws):
	""" Examine each transaction before it is committed, and if there are any mongodb data managers
	participating, add the needed commit hooks to support transactions correctly on those objects
	"""
	txn = kws['transaction']
	mongodms = filter(lambda f: type(f) == MongoDocument, txn._resources)
	dbs = set(map(lambda f: f.db, mongodms))
	for db in iter(dbs):
		txn.addBeforeCommitHook(mongoInitTxn_prehook, args=(), kws={'db':db, 'transaction':txn})
		txn.addAfterCommitHook(mongoConcludeTxn_posthook, args=(), kws={'db':db, 'transaction':txn})

def mongoInitTxn_prehook(*args, **kws):
	""" Called just before transaction is committed -- register transaction in db's 'transaction'
	collection. Ensure that any documents that are part of the current transaction are only associated
	with one data manager.
	"""
	db = kws['db']
	txn = kws['transaction']
	ActiveTransaction.transactionId = gen_transaction_id(txn)
	timestamp = datetime.datetime.utcnow()
	db.transactions.insert({'tid':ActiveTransaction.transactionId, 'state':'pending', 'date_created':timestamp, 'date_modified':timestamp})
	# list participating dm's, if not injective: dms->docs then call abort() here
	mongodms = filter(lambda f: type(f) == MongoDocument, txn._resources)
	txn_docIds = {}
	for dm in mongodms:
		if txn_docIds.has_key(dm.docId):
			logging.error('Dooming transaction: duplicate data managers for same document in single transaction!')
			txn.doom()
		txn_docIds[dm.docId] = 1

def mongoConcludeTxn_posthook(success, *args, **kws):
	""" Called immediately after a transaction is committed -- seal transaction state at 'done'/'failed'
	depending on result.
	"""
	db = kws['db']
	timestamp = datetime.datetime.utcnow()
	if success:
		db.transactions.update({'tid':ActiveTransaction.transactionId}, {'$set':{'state':'done', 'date_modified':timestamp}})
	else:
		db.transactions.update({'tid':ActiveTransaction.transactionId}, {'$set':{'state':'failed', 'date_modified':timestamp}})
	ActiveTransaction.transactionId = None # shouldn't matter, but just in case

class ActiveTransaction(object):
	""" Handle to the active transaction """
	transactionId = None

class MongoSavepoint(object):
	def __init__(self, dm):
		self.dm = dm
		self.saved_committed = self.dm.uncommitted.copy()
	
	def rollback(self):
		self.dm.uncommitted = self.saved_committed.copy()

class MongoDocument(object):
	transaction_manager = transaction.manager
	transactionsInitialized = False

	@classmethod
	def initialize(cls):
		""" This needs to be called once (e.g. at the beginning) for any transaction that will be
		interacting with mongodb (i.e. if a mongo data manager will be joining the transaction)
		"""
		txn = transaction.get()
		txn.addBeforeCommitHook(mongoListener_prehook, args=(), kws={'transaction':txn})
		cls.transactionsInitialized = True

	@classmethod
	def get_mongo_doc(cls, connection, dbname, colname, retrieve=None):
		""" Create a Mongodb data manager, join the current transaction, and return the dm.
		Use this convenience method to get a basic transaction-aware mongodb document
		"""
		dm = MongoDocument(connection, dbname, colname, retrieve)
		txn = transaction.get()
		txn.join(dm)
		return dm

	def __init__(self, connection, dbname, colname, retrieve=None):
		try:
			self.db = connection[dbname]
		except:
			logging.error('Cannot connect to Mongo server!')
			raise
		self.collection = self.db[colname]

		committed = {}
		if retrieve is not None:
			# if provided keys are not sufficient to retrieve unique document
			# or if no document returned, throw an exception here
			matchingdocs = self.collection.find(retrieve)
			if matchingdocs.count() == 0: raise Exception('Document not found!' + str(retrieve))
			if matchingdocs.count() > 1: raise Exception('Multiple matches for document, should be unique:' + str(retrieve))
			committed = matchingdocs.next()

		self.committed = committed
		self.uncommitted = self.committed.copy()
		# is _id unique across the entire database? If not, then use a sha hash of this concatenated with db id,
		# to make sure there are no false positives for duplicated dm's for same doc
		if self.uncommitted.has_key('_id'):
			self.docId = str(self.uncommitted['_id'])
		else:
			self.docId = None
	
	#
	# it's going to act like a dictionary so implement basic dictionary methods
	#

	def __getitem__(self, name):
		return self.uncommitted[name]

	def __setitem__(self, name, value):
		self.uncommitted[name] = value

	def __delitem__(self, name):
		del(self.uncommitted[name])

	def keys(self):
		return self.uncommitted.keys()

	def values(self):
		return self.uncommitted.values()

	def items(self):
		return self.uncommitted.items()

	def set(self, somedict):
		self.uncommitted = somedict # if somedict = None, this will delete the doc when the transaction is committed

	def __repr__(self):
		return repr(self.uncommitted)

	def has_key(self, key):
		return self.uncommitted.has_key(key)

	#
	# non-transactional manipulation:
	#

	def save(self, transaction):
		# commit new doc (replace existing doc) -- can be called manually outside of transactions
		if self.uncommitted == None: # document should be deleted
			if self.committed:
				self.collection.remove({'_id':self.committed['_id']})
			self.uncommitted = {}
		else:
			if self.committed:
				self.collection.update({'_id':self.committed['_id']}, self.uncommitted)
			else:
				self.collection.insert(self.uncommitted)

		self.committed = self.uncommitted.copy()

	def delete(self):
		if self.committed:
			self.collection.remove({'_id':self.committed['_id']})
		self.uncommitted = {}

	#
	# implement transaction protocol methods
	#

	def abort(self, transaction):
		self.uncommitted = self.committed.copy()
	
	def tpc_begin(self, transaction):
		if self.committed:
			self.collection.update({'_id':self.committed['_id']}, {'$push':{'pendingTransactions':ActiveTransaction.transactionId}})

	def commit(self, transaction):
		pass

	def tpc_vote(self, transaction):
		# check self.committed = current state
		# or there's a pending txn that's not this one
		# TODO: make this all atomic?
		if not MongoDocument.transactionsInitialized:
			raise Exception('MongoDB transactions not initialized correctly! Be sure to call MongoDocument.initialize() once at the start of each transaction.')
		if self.committed:
			if not self.committed.has_key('_id'):
				raise Exception('Committed document does not have an _id field!')
			dbcommitted = self.collection.find_one({'_id':self.committed['_id']})
			if not dbcommitted:
				raise TransientError('Document to be updated does not exist in database!')
			pendingTransactions = dbcommitted.pop('pendingTransactions')
			if self.committed.has_key('pendingTransactions'):
				raise TransientError('Concurrent modification! Transaction aborting...')
			if len(pendingTransactions) > 1 or pendingTransactions[0] != ActiveTransaction.transactionId:
				raise TransientError('Concurrent modification! Transaction aborting...')
			if dbcommitted != self.committed:
				raise TransientError('Concurrent modification! Transaction aborting...')

	def tpc_abort(self, transaction):
		self.uncommitted = self.committed.copy()
		if self.committed:
			self.collection.update({'_id':self.committed['_id']}, {'$pull':{'pendingTransactions':ActiveTransaction.transactionId}})
	
	def tpc_finish(self, transaction):
		self.save(transaction)

	def savepoint(self):
		return MongoSavepoint(self)
	
	def sortKey(self):
		return 'zzmongodm' + str(id(self)) # prioritize last since it's not "true" transactional

if __name__ == '__main__':
	(dbname, dbcol) = ('test_db', 'test_col')
	connection = MongoClient() # MongoClient('localhost', 27017)
	MongoDocument.initialize()
	dm = MongoDocument.get_mongo_doc(connection, dbname, dbcol, retrieve={'blah':'blrh'})
	print 'before: ' + str(dm)
	dm['blah'] = 'blrh'
	transaction.commit()
	print 'after: ' + str(dm)

