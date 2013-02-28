from datamanager import *

class MongoObject(MongoDocument):
	__requiredfields__ = ()
	__collection__ = None

	def __init__(self, connection, dbname, retrieve=None, transactional=True):
		self.connection = connection
		super(MongoObject, self).__init__(self.connection, dbname, self.__collection__, retrieve)
		if transactional:
			txn = transaction.get()
			txn.join(self)

	def tpc_vote(self, transaction):
		# do validation
		for field in self.__requiredfields__:
			if not self.has_key(field): raise Exception('Required field missing: ' + field)
		super(MongoObject, self).tpc_vote(transaction)

if __name__ == '__main__':
	from pymongo import MongoClient
	import transaction

	class User(MongoObject):
		__requiredfields__ = ('name', 'age')
		__collection__ = 'someusers'

	conn = MongoClient()
	MongoDocument.initialize()
	dbname = 'test_db'
	user = User(conn, dbname, retrieve={'name':'Sid'})
	user['name'] = 'Sid'
	user['age'] = 0
	transaction.commit()

