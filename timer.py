from time import clock


def timer(func):
    def wrapper(*args, **kwargs):
        before = clock()
        rv = func(*args, **kwargs)
        after = clock()
        print('%s elapsed %.7f' % (func.__name__, after - before))
        return rv
    return wrapper
