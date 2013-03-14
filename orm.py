from datamanager import *

class MongoObject(MongoDocument):
	__requiredfields__ = ()
	__collection__ = None

	def __init__(self, session, retrieve=None):
		self.session = session
		super(MongoObject, self).__init__(self.session, self.__collection__, retrieve)

	def tpc_vote(self, transaction):
		# do validation
		for field in self.__requiredfields__:
			if not self.has_key(field): raise Exception('Required field missing: ' + field)
		super(MongoObject, self).tpc_vote(transaction)

if __name__ == '__main__':
	import transaction
	from config import *

	class User(MongoObject):
		__requiredfields__ = ('name', 'age')
		__collection__ = 'users'

	dbname = 'test_db'
	session = Session(dbname)
	try:
		user = User(session, retrieve={'name':'Sid'})
	except:
		user = User(session)
	user['name'] = 'Sid'
	user['age'] = 0
	transaction.commit()
