class DBError(Exception):
    """
    Base class for DB related errors
    """

    pass


class DBObjectDoesntExistError(DBError):
    """
    When an expected Databsae Object does not exist
    """

    pass


class DBDeclarationError(DBError):
    """Errors relating to declartion of DB entities"""

    pass


class DBObjectMissingAttributeError(DBError):
    pass


class DBInvalidAttributeError(DBError):
    pass


class DBNotAlteredAttributeError(DBError):
    pass


class InvalidDBEntityChildError(DBError):
    pass


class DatabaseInitialisationError(DBError):
    """When tables for feed/operation have not yet been initialised"""

    pass


class MissingDBEntityChildError(DBDeclarationError):
    pass


class AlreadyAssignedDBEntityError(DBDeclarationError):
    pass


class DBEntityChildAlreadyExitsError(DBDeclarationError):
    pass
