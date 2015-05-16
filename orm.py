from datamanager import MongoDocument
import logging

class ORMValidationError(Exception):
	pass

class MongoObject(MongoDocument):
	__requiredfields__ = ()
	__collection__ = None

	def __init__(self, session, retrieve=None):
		self.session = session
		super(MongoObject, self).__init__(self.session, self.__collection__, retrieve)

	def validate(self):
		if self.uncommitted:
			for field in self.__requiredfields__:
				if not self.has_key(field): raise ORMValidationError('Required field missing: ' + field)

	def tpc_vote(self, transaction):
		self.validate()
		super(MongoObject, self).tpc_vote(transaction)

	def save(self):
		if not self.session.transactional:
			self.validate()
		super(MongoObject, self).save()

if __name__ == '__main__':
	import transaction
	from config import Session
	logging.basicConfig()
	logging.getLogger('datamanager').setLevel(logging.DEBUG)

	class User(MongoObject):
		__requiredfields__ = ('name', 'age')
		__collection__ = 'users'

	dbname = 'test_db'
	session = Session(dbname)
	try:
		user1 = User(session, retrieve={'name':'Sid'})
	except:
		user1 = User(session)
	try:
		user2 = User(session, retrieve={'name':'Dan'})
	except:
		user2 = User(session)
	user1['name'] = 'Sid'
	user1['age'] = 0
	user2['name'] = 'Dan'
	user2['age'] = 0
	user1['friend'] = user2
	transaction.commit()
