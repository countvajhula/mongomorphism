""" Unit/Integration tests """

import unittest
from orm import MongoObject
from mongomorphism.exceptions import ORMValidationError
import transaction

colname = 'test_collection'

class Sample(MongoObject):
	__collection__ = colname
	__requiredfields__ = ('field1', 'field2')

class SessionStub(object):
	db = {colname: None}
	transactional = True

class GoodInput(unittest.TestCase):
	""" Check that fields are validated correctly when saving a MongoObject
	"""

	def tearDown(self):
		transaction.abort()

	def test_validation_should_succeed_when_all_required_fields_present(self):
		session = SessionStub()
		session.transactional = False
		obj = Sample(session)
		obj['field1'] = 'something'
		obj['field2'] = 'something else'
		self.assertIsNone(obj.validate()) # no exception should be raised here

class BadInput(unittest.TestCase):

	def tearDown(self):
		transaction.abort()

	def test_validation_should_fail_when_required_field_missing(self):
		session = SessionStub()
		session.transactional = False
		obj = Sample(session)
		obj['field1'] = 'something'
		self.assertRaises(ORMValidationError, obj.validate)

	def test_save_ignored_on_transactional_document(self):
		session = SessionStub()
		obj = Sample(session)
		obj['field1'] = 'something'
		obj['field2'] = 'something else'
		obj.save() # no exception should be raised here
		self.assertNotEqual(obj.committed, obj.uncommitted)
		self.assertIn(obj, transaction.get()._resources)

class EdgeCases(unittest.TestCase):
	pass

