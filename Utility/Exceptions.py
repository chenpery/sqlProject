class _Exceptions(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


# exceptions classes, you can print the exception using print(exception)
class DatabaseException(_Exceptions):
    class ConnectionInvalid(_Exceptions):
        print(_Exceptions)

    class NOT_NULL_VIOLATION(_Exceptions):
        print(_Exceptions)

    class FOREIGN_KEY_VIOLATION(_Exceptions):
        print(_Exceptions)

    class UNIQUE_VIOLATION(_Exceptions):
        print(_Exceptions)

    class CHECK_VIOLATION(_Exceptions):
        print(_Exceptions)

    class database_ini_ERROR(_Exceptions):
        print(_Exceptions)

    class UNKNOWN_ERROR(_Exceptions):
        print(_Exceptions)
