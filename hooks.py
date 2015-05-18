""" Transaction hooks """

import datetime
from bson.dbref import DBRef
import support
from mongomorphism.exceptions import DuplicateDataManagersError
import logging
import transaction


logger = logging.getLogger(__name__)


def mongo_listener_prehook(*args, **kws):
	""" Examine each transaction before it is committed, and if there are any
	mongodb data managers participating, add the needed commit hooks to support
	transactions correctly on those objects.
	"""
	txn = transaction.get()
	mongodms = filter(lambda f:
	                  hasattr(f, 'mongo_data_manager'),
	                  txn._resources)
	sessions = set(map(lambda f: f.session, mongodms))
	for session in iter(sessions):
		txn.addBeforeCommitHook(mongo_transaction_prehook, args=(),
		                        kws={'session':session})
		txn.addAfterCommitHook(mongo_transaction_posthook,
		                       args=(), kws={'session':session})

def mongo_listener_posthook(*args, **kws):
	""" Reinitialize session (i.e. add mongo listener to transaction) for
	subsequent transaction.
	"""
	session = kws['session']
	session.active = False
	# so that document can continue being used transactionally
	# without manual reinitialization:
	session.begin()

def mongo_transaction_prehook(*args, **kws):
	""" Initialize transaction. Called just before transaction is committed.
	Register transaction in db's 'transaction' collection. Ensure that any
	documents that are part of the current transaction are only associated
	with one data manager.
	"""
	db = kws['session'].db
	txn = transaction.get()
	support.ActiveTransaction.transaction_id = support.gen_transaction_id(txn)
	timestamp = datetime.datetime.utcnow()
	db.transactions.insert({'tid': support.ActiveTransaction.transaction_id,
	                        'state': 'pending',
	                        'date_created': timestamp,
	                        'date_modified': timestamp})
	# list participating dm's, if not injective: dms->docs
	# then call abort() here
	mongodms = filter(lambda f:
	                  hasattr(f, 'mongo_data_manager'),
	                  txn._resources)
	txn_doc_ids = {}
	for dm in mongodms:
		if dm.doc_id:
			if txn_doc_ids.has_key(dm.doc_id):
				raise DuplicateDataManagersError('Aborting transaction:'
				    ' duplicate data managers for same document'
					' in single transaction!')
			txn_doc_ids[dm.doc_id] = 1

def mongo_transaction_posthook(success, *args, **kws):
	""" Conclude transaction. Called immediately after a transaction is
	committed. If transaction succeeded, perform any pending queued operations.
	Seal transaction state at 'done'/'failed'.
	"""
	session = kws['session']
	db = session.db
	timestamp = datetime.datetime.utcnow()
	if success:
		# perform queued operations
		if session.queue:
			logger.debug('performing queued operations')
			def update_refs(doc):
				col = doc.collection
				queued = doc.queued
				for key,doc_ref in queued.items():
					ref = DBRef(doc_ref.collection.name, doc_ref['_id'])
					col.update({'_id':doc['_id']}, {'$set':{key:ref}})
				doc.queued = {}
			for doc in session.queue:
				update_refs(doc)
			session.queue = []
		db.transactions.update(
		    {'tid': support.ActiveTransaction.transaction_id},
		    {'$set': {'state': 'done',
		             'date_modified': timestamp}})
	else:
		db.transactions.update(
		    {'tid': support.ActiveTransaction.transaction_id},
		    {'$set': {'state': 'failed',
		              'date_modified': timestamp}})
	# shouldn't matter, but just in case:
	support.ActiveTransaction.transaction_id = None

