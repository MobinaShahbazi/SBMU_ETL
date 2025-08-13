from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r took: %2.4f sec' % (f.__name__, te-ts))
        return result
    return wrap


def find(element, json):

    rv = json

    if not element:
        return json
    else:
        keys = element.split('.')
        for key in keys:
            rv = rv[key]
        return rv