""" Unit/Integration tests """

import unittest
from datamanager import MongoDocument
import transaction

colname = 'test_collection'

class SessionStub(object):
	db = {colname: None}
	transactional = True

class Transactional_GoodInput(unittest.TestCase):

	def tearDown(self):
		transaction.abort()

	def test_should_enter_transaction_when_data_added(self):
		session = SessionStub()
		doc = MongoDocument(session, colname)
		doc['name'] = 'Saruman'
		self.assertIn(doc, transaction.get()._resources)

	def test_should_enter_transaction_when_data_deleted(self):
		session = SessionStub()
		doc = MongoDocument(session, colname)
		doc.committed = {'name': 'Saruman', 'profession': 'wizard'}
		doc.uncommitted = doc.committed.copy()
		del doc['profession']
		self.assertIn(doc, transaction.get()._resources)

	def test_should_enter_transaction_when_contents_set(self):
		session = SessionStub()
		doc = MongoDocument(session, colname)
		doc.set({'name': 'Saruman'})
		self.assertIn(doc, transaction.get()._resources)

	def test_should_enter_transaction_when_deleted(self):
		session = SessionStub()
		doc = MongoDocument(session, colname)
		doc.delete()
		self.assertIn(doc, transaction.get()._resources)

class Transactional_BadInput(unittest.TestCase):

	def tearDown(self):
		transaction.abort()

	def test_save_ignored_on_transactional_document(self):
		session = SessionStub()
		doc = MongoDocument(session, colname)
		doc['name'] = 'Saruman'
		doc.save()
		self.assertNotEqual(doc.committed, doc.uncommitted)
		self.assertIn(doc, transaction.get()._resources)

class Transactional_EdgeCases(unittest.TestCase):
	pass

class NonTransactional_GoodInput(unittest.TestCase):
	pass

class NonTransactional_BadInput(unittest.TestCase):
	pass

class NonTransactional_EdgeCases(unittest.TestCase):
	pass

