import unittest
from datamanager import MongoDocument
from mongomorphism.exceptions import (
		DocumentMatchNotUniqueError,
		DocumentNotFoundError,
		DuplicateDataManagersError,
		)
from config import Session
from pymongo import MongoClient
import transaction

dbname = '_test_db'
colname = '_test_col'

class Transactional_GoodInput(unittest.TestCase):

	def setUp(self):
		self.session = Session(dbname)
		self.doc = MongoDocument(self.session, colname)

	def tearDown(self):
		transaction.abort()
		conn = MongoClient()
		conn.drop_database(dbname)

	def test_data_equivalence_after_transaction_committed(self):
		self.doc['name'] = 'Saruman'
		transaction.commit()
		doc2 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		self.assertEqual(self.doc.uncommitted, self.doc.committed)
		self.assertEqual(self.doc.committed, doc2.committed)

	def test_documents_should_be_persisted_only_after_transaction_committed(self):
		self.doc['name'] = 'Saruman'
		self.doc.save()
		self.assertRaises(DocumentNotFoundError, MongoDocument, self.session, colname, retrieve={'name':'Saruman'})
		transaction.commit()
		doc2 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		self.assertEqual(self.doc.committed, doc2.committed)

	def test_should_correctly_persist_documents_in_multi_document_transaction(self):
		self.doc['firstname'] = 'Saruman'
		self.doc['lastname'] = 'theWhite'
		doc2 = MongoDocument(self.session, colname)
		doc2['firstname'] = 'Saruman'
		doc2['lastname'] = 'theGreen'
		transaction.commit()
		doc3 = MongoDocument(self.session, colname, retrieve={'firstname':'Saruman', 'lastname': 'theWhite'})
		doc4 = MongoDocument(self.session, colname, retrieve={'firstname':'Saruman', 'lastname': 'theGreen'})
		self.assertEqual(self.doc.committed, doc3.committed)
		self.assertEqual(doc2.committed, doc4.committed)

	def test_deleted_documents_should_be_deleted(self):
		self.doc['name'] = 'Saruman'
		transaction.commit()
		self.doc.delete()
		transaction.commit()
		self.assertRaises(DocumentNotFoundError, MongoDocument, self.session, colname, retrieve={'name':'Saruman'})

class Transactional_BadInput(unittest.TestCase):

	def setUp(self):
		self.session = Session(dbname)
		self.doc = MongoDocument(self.session, colname)

	def tearDown(self):
		transaction.abort()
		conn = MongoClient()
		conn.drop_database(dbname)

	def test_should_raise_error_if_duplicate_data_managers_are_used_on_the_same_document(self):
		self.doc['name'] = 'Saruman'
		transaction.commit()
		doc2 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		doc3 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		doc2['profession'] = 'wizard'
		doc3['profession'] = 'warlock'
		self.assertRaises(DuplicateDataManagersError, transaction.commit)

class Transactional_EdgeCases(unittest.TestCase):

	def setUp(self):
		self.session = Session(dbname)
		self.doc = MongoDocument(self.session, colname)

	def tearDown(self):
		transaction.abort()
		conn = MongoClient()
		conn.drop_database(dbname)

	def test_document_should_retain_transactional_status_after_commit(self):
		self.doc['name'] = 'Saruman'
		transaction.commit()
		self.assertTrue(self.doc.session.transactional)
		self.assertTrue(self.doc.session.transactionInitialized)

	def test_deleted_documents_should_be_empty(self):
		self.doc['name'] = 'Saruman'
		transaction.commit()
		self.doc.delete()
		transaction.commit()
		self.assertEqual(self.doc.uncommitted, {})
		self.assertEqual(self.doc.committed, {})

	def test_should_be_able_to_use_session_across_transactions(self):
		self.doc['name'] = 'Saruman'
		doc2 = MongoDocument(self.session, colname)
		self.assertEqual(transaction.get()._resources, [self.doc]) # doc2 not part of this transaction since no data
		transaction.commit()
		doc2['name'] = 'Gandalf'
		self.assertEqual(transaction.get()._resources, [doc2]) # doc not part of this transaction since no changes
		transaction.commit()
		self.doc['profession'] = 'wizard'
		doc2['profession'] = 'wizardToo'
		self.assertEqual(transaction.get()._resources, [self.doc, doc2]) # both should be part of this transaction
		doc3 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		doc4 = MongoDocument(self.session, colname, retrieve={'name':'Gandalf'})
		self.assertEqual(self.doc.committed, doc3.committed)
		self.assertEqual(doc2.committed, doc4.committed)

class NonTransactional_GoodInput(unittest.TestCase):

	def setUp(self):
		self.session = Session(dbname, transactional=False)
		self.doc = MongoDocument(self.session, colname)

	def tearDown(self):
		conn = MongoClient()
		conn.drop_database(dbname)

	def test_data_equivalence_after_persisting(self):
		self.doc['name'] = 'Saruman'
		self.doc.save()
		doc2 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		self.assertEqual(self.doc.uncommitted, self.doc.committed)
		self.assertEqual(self.doc.committed, doc2.committed)

	def test_documents_should_be_persisted_on_calling_save_and_retrieved_correctly(self):
		self.doc['name'] = 'Saruman'
		self.doc.save()
		self.assertIn('_id', self.doc.committed)
		doc2 = MongoDocument(self.session, colname, retrieve={'name':'Saruman'})
		self.assertEqual(self.doc.committed, doc2.committed) # should now both include mongodb _id

	def test_deleted_documents_should_be_deleted(self):
		self.doc['name'] = 'Saruman'
		self.doc.save()
		self.doc.delete()
		self.assertRaises(DocumentNotFoundError, MongoDocument, self.session, colname, retrieve={'name':'Saruman'})

class NonTransactional_BadInput(unittest.TestCase):

	def setUp(self):
		self.session = Session(dbname, transactional=False)
		self.doc = MongoDocument(self.session, colname)

	def tearDown(self):
		conn = MongoClient()
		conn.drop_database(dbname)

	def test_should_raise_error_if_retrieve_spec_does_not_find_document(self):
		self.assertRaises(DocumentNotFoundError, MongoDocument, self.session, colname, retrieve={'firstname':'Saruman'})

	def test_should_raise_error_if_retrieve_spec_does_not_find_unique_document(self):
		self.doc['firstname'] = 'Saruman'
		self.doc['lastname'] = 'theWhite'
		self.doc.save()
		doc2 = MongoDocument(self.session, colname)
		doc2['firstname'] = 'Saruman'
		doc2['lastname'] = 'theGreen'
		doc2.save()
		self.assertRaises(DocumentMatchNotUniqueError, MongoDocument, self.session, colname, retrieve={'firstname':'Saruman'})

class NonTransactional_EdgeCases(unittest.TestCase):

	def setUp(self):
		self.session = Session(dbname, transactional=False)
		self.doc = MongoDocument(self.session, colname)

	def tearDown(self):
		conn = MongoClient()
		conn.drop_database(dbname)

	def test_document_should_retain_nontransactional_status_after_commit(self):
		self.doc['name'] = 'Saruman'
		transaction.commit()
		self.assertFalse(self.doc.session.transactional)
		self.assertFalse(self.doc.session.transactionInitialized)

	def test_deleted_documents_should_be_empty(self):
		self.doc['name'] = 'Saruman'
		self.doc.save()
		self.doc.delete()
		self.assertEqual(self.doc.uncommitted, {})
		self.assertEqual(self.doc.committed, {})

