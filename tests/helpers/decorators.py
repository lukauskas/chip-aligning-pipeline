import os
import unittest


def slow():
    if 'CHIPALIGN_SLOW_TESTS' in os.environ:
        return lambda func: func
    return unittest.skip("Slow tests are off")