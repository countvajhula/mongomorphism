"""
TODO: mirror datamanager functional tests
"""
import unittest
from orm import MongoObject
from orm import ORMValidationError
from config import Session
from pymongo import MongoClient

dbname = '_test_db'
colname = '_test_col'

class Sample(MongoObject):
	__collection__ = colname
	__requiredfields__ = ('field1', 'field2')

class Transactional_GoodInput(unittest.TestCase):
	pass

class Transactional_BadInput(unittest.TestCase):
	pass

class Transactional_EdgeCases(unittest.TestCase):
	pass

class NonTransactional_GoodInput(unittest.TestCase):

	def tearDown(self):
		conn = MongoClient()
		conn.drop_database(dbname)

	def testValidationSucceedsWhenAllRequiredFieldsPresent(self):
		session = Session(dbname, transactional=False)
		obj = Sample(session)
		obj['field1'] = 'something'
		obj['field2'] = 'something else'
		obj.save() # no exception should be raised here
		obj2 = Sample(session, retrieve={'field1': 'something'})
		self.assertEqual(obj.committed, obj2.committed)

class NonTransactional_BadInput(unittest.TestCase):

	def tearDown(self):
		conn = MongoClient()
		conn.drop_database(dbname)

	def testValidationFailsWhenRequiredFieldMissing(self):
		session = Session(dbname, transactional=False)
		obj = Sample(session)
		obj['field1'] = 'something'
		self.assertRaises(ORMValidationError, obj.save)

class NonTransactional_EdgeCases(unittest.TestCase):
	pass

