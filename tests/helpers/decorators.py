import os
from nose.tools import nottest

def slow(func):
    if 'CHIPALIGN_SLOW_TESTS' in os.environ:
        return func
    return nottest(func)