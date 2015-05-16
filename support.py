""" Transaction support stuff: UID, aspects """

import datetime
import platform
from hashlib import sha256
import functools
import logging


logger = logging.getLogger(__name__)


def gen_transaction_id(txn):
	""" Generate a globally unique id for a transaction
	TODO: mongo _id is globally unique so can just use that instead
	"""
	timestamp = str(datetime.datetime.utcnow()) # particular moment in time
	local_id = str(id(txn)) # guaranteed to be unique on this machine (but not all machines concurrently acting) at the present moment
	host_id = platform.node() # hostname as something 'globally unique'
	# alternatively MAC address, not sure if reliable: from uuid import getnode; mac = getnode()
	global_id = sha256("|".join((timestamp, local_id, host_id))).hexdigest()
	return global_id # repeated calls for the same transaction would NOT return the same ID - should be called only to generate a unique ID

class ActiveTransaction(object):
	""" Handle to the active transaction """
	transactionId = None

def mutative_operation(func):
	""" For any operation that changes the document, join current transaction
	if in transactional mode.
	"""
	@functools.wraps(func)
	def wrapper(*args, **kwargs):
		self = args[0]
		if self.session.transactional:
			self._join_transaction_if_necessary()
		return func(*args, **kwargs)
	return wrapper

