
# Data manager errors

class DocumentNotFoundError(Exception):
	pass

class DocumentMatchNotUniqueError(Exception):
	pass

class DuplicateDataManagersError(Exception):
	pass


# ORM errors

class ORMValidationError(Exception):
	pass

# Transactional errors

class SessionNotInitializedError(Exception):
	pass

