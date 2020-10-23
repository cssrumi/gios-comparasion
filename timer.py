from time import clock


def timer(func):
    def wrapper(*args, **kwargs):
        before = clock()
        rv = func(*args, **kwargs)
        after = clock()
        print('%s function processed after %.7f' % (func.__name__, after - before))
        return rv
    return wrapper
