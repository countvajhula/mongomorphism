#!/usr/bin/env python

import unittest
from orm import MongoObject
from config import Session
from pymongo import MongoClient

class ORMValidationTester(unittest.TestCase):
	""" Check that fields are validated correctly when saving a MongoObject
	"""

	def setUp(self):
		self.dbname = '_test_db'
		self.colname = '_test_col'

	def tearDown(self):
		conn = MongoClient()
		conn.drop_database(self.dbname)

	def testValidationFailsWhenRequiredFieldMissing(self):
		colname = self.colname
		class Sample(MongoObject):
			__collection__ = colname
			__requiredfields__ = ('field1', 'field2')

		session = Session(self.dbname, transactional=False)
		obj = Sample(session)
		obj['field1'] = 'something'
		self.assertRaises(Exception, obj.save)

	def testValidationSucceedsWhenAllRequiredFieldsPresent(self):
		colname = self.colname
		class Sample(MongoObject):
			__collection__ = colname
			__requiredfields__ = ('field1', 'field2')

		session = Session(self.dbname, transactional=False)
		obj = Sample(session)
		obj['field1'] = 'something'
		obj['field2'] = 'something else'
		# no exception should be raised here
		obj.save()
