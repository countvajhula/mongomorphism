from mongodm import *

class MongoObject(MongoDocument):
	__requiredfields__ = ()
	__collection__ = None

	def __init__(self, connection, dbname, retrieve=None, transactional=True):
		self.connection = connection
		super(MongoDocument, self).__init__(self.connection, dbname, self.__collection__, retrieve)
		if transactional:
			txn = transaction.get()
			txn.join(self)

	def tpc_vote(self):
		# do validation
		for field in self.__requiredfields__:
			if not self.has_key(field): raise Exception('Required field missing: ' + field)
		super(MongoDocument, self).tpc_vote()
