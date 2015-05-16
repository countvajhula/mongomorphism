import logging
from bson.dbref import DBRef
from bson import BSON
import jsonpickle
import transaction
from transaction.interfaces import TransientError
import support
from support import mutative_operation
from mongomorphism.exceptions import (
		DocumentNotFoundError,
		DocumentMatchNotUniqueError,
		)


logger = logging.getLogger(__name__)


class MongoSavepoint(object):
	def __init__(self, dm):
		self.dm = dm
		self.saved_committed = self.dm.uncommitted.copy()
	
	def rollback(self):
		self.dm.uncommitted = self.saved_committed.copy()

class MongoDocument(object):
	""" A Mongodb data manager. A MongoDocument represents a document in mongo database,
	and acts like a regular python dict. By default the document will be transaction-aware,
	providing "ACID-like" functionality on top of mongodb by interfacing with the
	python 'transaction' package. Changes will be persisted only if the transaction succeeds.
	If non-transactional, then save() and delete() methods may be used.
	"""

	transaction_manager = transaction.manager
	mongo_data_manager = True # internal: for transaction hook injection

	def __init__(self, session, colname, retrieve=None):
		""" Note, if using this as a data manager for the python transaction package,
		by default this will automatically join the current transaction. If you'd like to do
		it manually, set transactional=False here
		"""
		try:
			self.session = session
			self.collection = self.session.db[colname]
		except:
			logger.error('Cannot connect to Mongo server!')
			raise

		committed = {}
		if retrieve is not None:
			# if provided keys are not sufficient to retrieve unique document
			# or if no document returned, throw an exception here
			matchingdocs = self.collection.find(retrieve)
			if matchingdocs.count() == 0: raise DocumentNotFoundError('Document not found!' + str(retrieve))
			if matchingdocs.count() > 1: raise DocumentMatchNotUniqueError('Multiple matches for document, should be unique:' + str(retrieve))
			committed = matchingdocs.next()

		self.committed = committed
		self.uncommitted = self.committed.copy()
		self.queued = {}

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
		if isinstance(self.uncommitted[name], DBRef):
			# if referenced doc is part of current transaction return that instance
			# otherwise create a new MongoDocument instance and return that
			txn = transaction.get()
			doc = self.session.db.dereference(self.uncommitted[name])
			livedocs = filter(lambda f: f.has_key('_id') and f['_id'] == doc['_id'], txn._resources)
			if livedocs:
				return livedocs[0]
			return MongoDocument(self.session, self.collection.name, retrieve=doc)
		else:
			try:
				value = jsonpickle.decode(self.uncommitted[name])
			except:
				value = self.uncommitted[name]
			return value

	@mutative_operation
	def __setitem__(self, name, value):
		if hasattr(value, 'mongo_data_manager'):
			if value.has_key('_id'):
				self.uncommitted[name] = DBRef(value.collection.name, value['_id'])
			else:
				txn = transaction.get()
				if value in txn._resources:
					# this document is part of the current transaction and doesn't have a mongo _id yet
					# queue it and trigger adding the reference at the end of the transaction
					self.queued[name] = value
				else:
					# this document is not part of the current transaction, so treat it as a regular
					# python dict and make it an embedded document inside the current doc
					logger.warn('mongo document does not exist in mongodb and is not part of current transaction - saving as embedded instead of a reference')
					self.uncommitted[name] = value.copy()
		else:
			try:
				BSON.encode({name:value})
				self.uncommitted[name] = value
			except:
				self.uncommitted[name] = jsonpickle.encode(value)

	@mutative_operation
	def __delitem__(self, name):
		del(self.uncommitted[name])

	def keys(self):
		return self.uncommitted.keys()

	def values(self):
		return self.uncommitted.values()

	def items(self):
		return self.uncommitted.items()

	def copy(self):
		return self.uncommitted.copy()

	@mutative_operation
	def set(self, somedict):
		""" Set the document to be equal to the provided dict """
		self.uncommitted = somedict # if somedict = None, this will delete the doc when the transaction is committed. alternatively, delete() can be called which does the same thing.

	def __repr__(self):
		return repr(self.uncommitted)

	def __len__(self):
		return len(self.uncommitted)

	def has_key(self, key):
		return self.uncommitted.has_key(key)

	def _save(self):
		# commit new doc (replace existing doc) -- can be called manually outside of transactions
		if self.uncommitted == None: # document should be deleted
			self._delete()
		else:
			if self.committed:
				self.collection.update({'_id':self.committed['_id']}, self.uncommitted)
			else:
				self.collection.insert(self.uncommitted)
		# if there are queued changes that cannot be completed in this transaction
		# add them to the session queue to be performed after the transaction
		if self.queued:
			self.session.queue.append(self)

		self.committed = self.uncommitted.copy()

	def _delete(self):
		if self.committed:
			self.collection.remove({'_id':self.committed['_id']})
		self.uncommitted = {}

	#
	# non-transactional manipulation:
	#

	def save(self):
		if self.session.transactional:
			logger.warn('save() called on transactional document. ignoring...')
		else:
			self._save()

	@mutative_operation
	def delete(self):
		if self.session.transactional:
			self.uncommitted = None
		else:
			self._delete()
			self.committed = self.uncommitted.copy()

	#
	# implement transaction protocol methods
	#

	def _join_transaction_if_necessary(self):
		""" Join current transaction if document is not already part of it.
		"""
		txn = transaction.get()
		if not self in txn._resources:
			txn.join(self)

	def abort(self, transaction):
		self.uncommitted = self.committed.copy()
	
	def tpc_begin(self, transaction):
		if self.committed:
			self.collection.update({'_id':self.committed['_id']}, {'$push':{'pendingTransactions':support.ActiveTransaction.transactionId}})

	def commit(self, transaction):
		pass

	def tpc_vote(self, transaction):
		# check self.committed = current state
		# or there's a pending txn that's not this one
		if not self.session.transactionInitialized:
			raise Exception('MongoDB transactions not initialized correctly! Be sure to create a new session instance or call session.initialize() once at the start of each transaction.')
		if self.uncommitted:
			# validate new data
			keytypes = map(lambda f:type(f), self.uncommitted.keys())
			invalidkeys = filter(lambda f:f != str and f != unicode, keytypes)
			if invalidkeys:
				raise Exception('Invalid key: Documents must have only string or unicode keys!')
			try:
				BSON.encode(self.uncommitted) # final check that document is BSON-compatible
			except:
				raise

		if self.committed:
			if not self.committed.has_key('_id'):
				raise Exception('Committed document does not have an _id field!') # this should never happen (if it does then we're in trouble - tpc_abort will fail)
			dbcommitted = self.collection.find_one({'_id':self.committed['_id']})
			if not dbcommitted:
				raise TransientError('Document to be updated does not exist in database!')
			pendingTransactions = dbcommitted.pop('pendingTransactions')
			if self.committed.has_key('pendingTransactions'):
				raise TransientError('Concurrent modification! Transaction aborting...')
			if len(pendingTransactions) > 1 or pendingTransactions[0] != support.ActiveTransaction.transactionId:
				raise TransientError('Concurrent modification! Transaction aborting...')
			if dbcommitted != self.committed:
				raise TransientError('Concurrent modification! Transaction aborting...')

	def tpc_abort(self, transaction):
		self.uncommitted = self.committed.copy()
		if self.committed:
			self.collection.update({'_id':self.committed['_id']}, {'$pull':{'pendingTransactions':support.ActiveTransaction.transactionId}})
			dbcommitted = self.collection.find_one({'_id':self.committed['_id']})
			if dbcommitted.has_key('pendingTransactions') and not dbcommitted['pendingTransactions']:
				self.collection.update({'_id':self.committed['_id']}, {'$unset':{'pendingTransactions':1}})
	
	def tpc_finish(self, transaction):
		self._save()

	def savepoint(self):
		return MongoSavepoint(self)
	
	def sortKey(self):
		return 'zzmongodm' + str(id(self)) # prioritize last since it's not "true" transactional

if __name__ == '__main__':
	from config import Session
	logging.basicConfig()
	logger.setLevel(logging.DEBUG)
	(dbname, dbcol) = ('test_db', 'test_col')
	session = Session(dbname)
	try:
		dm = MongoDocument(session, dbcol, retrieve={'foo':'bar'})
	except:
		try:
			dm = MongoDocument(session, dbcol, retrieve={'foo':'BAR'})
		except:
			dm = MongoDocument(session, dbcol)
	print 'before: ' + str(dm)
	if len(dm) > 0:
		swapcase = lambda v:v.islower() and v.upper() or v.lower()
		for k,v in dm.items():
			if k == '_id': continue
			dm[k] = swapcase(v)
	else:
		dm['foo'] = 'bar'
		dm['baz'] = 'bobo'
	transaction.commit()
	print 'after: ' + str(dm)
