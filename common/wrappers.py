import logging
import sys
# TODO make a config item
logging.basicConfig(filename='/Users/rirwin/theft-market/server/log/datastore_access_timing.log',level=logging.INFO)

class logger(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, *args, **kw):
        logging.info('Entering %s' % self.func)
        return self.func(*args, **kw)


class general_function_handler(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, *args, **kw):
        try:
            retval = self.func(*args, **kw)
        except Exception, e :
            logging.warning('Exception in %s' % self.func)
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(e).__name__, e.args)
            logging.exception(message)
            retval = None
            # probably should sys.exit(1) for certain exception types
            # like AttributeError, KeyError, NameError, ProgrammingError
            sys.exit(1)
        return retval
    
class database_function_handler(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, obj, type=None):
        return self.__class__(self.func.__get__(obj, type))
    def __call__(self, conn, *args, **kw):
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN")
            retval = self.func(cursor, *args, **kw)
            cursor.execute("COMMIT")
        except Exception, e :
            logging.warning('Exception in %s' % self.func)
            template = "An exception of type {0} occured. Arguments:\n{1!r}"
            message = template.format(type(e).__name__, e.args)
            logging.exception(message)
            cursor.execute("ROLLBACK")
            retval = None
        finally:
            cursor.close()

        return retval
