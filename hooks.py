""" Transaction hooks """

import datetime
from bson.dbref import DBRef
import support
from mongomorphism.exceptions import DuplicateDataManagersError
import logging
import transaction


logger = logging.getLogger(__name__)


def mongoListener_prehook(*args, **kws):
	""" Examine each transaction before it is committed, and if there are any mongodb data managers
	participating, add the needed commit hooks to support transactions correctly on those objects
	"""
	txn = transaction.get()
	mongodms = filter(lambda f: hasattr(f, 'mongo_data_manager'), txn._resources)
	sessions = set(map(lambda f: f.session, mongodms))
	for session in iter(sessions):
		txn.addBeforeCommitHook(mongoInitTxn_prehook, args=(), kws={'session':session})
		txn.addAfterCommitHook(mongoConcludeTxn_posthook, args=(), kws={'session':session})

def mongoListener_posthook(*args, **kws):
	""" Mark session transactionInitialized as false
	"""
	session = kws['session']
	session.transactionInitialized = False
	session.initialize() # so that document can continue being used transactionally without manual reinitialization

def mongoInitTxn_prehook(*args, **kws):
	""" Called just before transaction is committed -- register transaction in db's 'transaction'
	collection. Ensure that any documents that are part of the current transaction are only associated
	with one data manager.
	"""
	db = kws['session'].db
	txn = transaction.get()
	support.ActiveTransaction.transactionId = support.gen_transaction_id(txn)
	timestamp = datetime.datetime.utcnow()
	db.transactions.insert({'tid':support.ActiveTransaction.transactionId, 'state':'pending', 'date_created':timestamp, 'date_modified':timestamp})
	# list participating dm's, if not injective: dms->docs then call abort() here
	mongodms = filter(lambda f: hasattr(f, 'mongo_data_manager'), txn._resources)
	txn_docIds = {}
	for dm in mongodms:
		if dm.docId:
			if txn_docIds.has_key(dm.docId):
				raise DuplicateDataManagersError('Aborting transaction: duplicate data managers for same document in single transaction!')
			txn_docIds[dm.docId] = 1

def mongoConcludeTxn_posthook(success, *args, **kws):
	""" Called immediately after a transaction is committed: If transaction succeeded, perform any pending
	queued operations. Seal transaction state at 'done'/'failed'.
	"""
	session = kws['session']
	db = session.db
	timestamp = datetime.datetime.utcnow()
	if success:
		# perform queued operations
		if session.queue:
			logger.debug('performing queued operations')
			def updateRefs(doc):
				col = doc.collection
				queued = doc.queued
				for key,doc_ref in queued.items():
					ref = DBRef(doc_ref.collection.name, doc_ref['_id'])
					col.update({'_id':doc['_id']}, {'$set':{key:ref}})
				doc.queued = {}
			for doc in session.queue:
				updateRefs(doc)
			session.queue = []
		db.transactions.update({'tid':support.ActiveTransaction.transactionId}, {'$set':{'state':'done', 'date_modified':timestamp}})
	else:
		db.transactions.update({'tid':support.ActiveTransaction.transactionId}, {'$set':{'state':'failed', 'date_modified':timestamp}})
	support.ActiveTransaction.transactionId = None # shouldn't matter, but just in case

