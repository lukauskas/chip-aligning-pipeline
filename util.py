from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from contextlib import contextmanager
from functools import wraps
import tempfile
import os
import shutil

@contextmanager
def temporary_directory(logger=None, cleanup_on_exception=False, *args, **kwargs):
    """
    A context manager to change the current working directory to a temporary directory,
    and change it back, removing the temporary directory afterwards

    :param logger: if set to non none, the code will call logger.debug() to write which directory it is working in
    :type logger: :class:`logging.Logger`
    :param cleanup_on_exception: If set to true, the temporary directory will be removed even when exception occurs
    :type cleanup_on_exception: bool
    :param args: args to pass to :func:`tempfile.mkdtemp`
    :param kwargs: kwargs to pass to :func:`tempfile.mkdtemp`
    :return:
    """
    current_working_directory = os.getcwd()
    temp_dir = tempfile.mkdtemp(*args, **kwargs)
    try:
        logger.debug('Working on: {}'.format(temp_dir))
        os.chdir(temp_dir)
        yield temp_dir
    except:
        if cleanup_on_exception:
            # If exception, and cleanup_on_exception is set -- remove directory
            logger.debug('Removing {} as cleanup_on_exception is set'.format(cleanup_on_exception))
            shutil.rmtree(temp_dir)
        raise
    finally:
        os.chdir(current_working_directory)

    # No exception case - remove always
    logger.debug('Removing {}'.format(temp_dir))
    shutil.rmtree(temp_dir)


def memoised(f):
    """
    A wrapper that memoises the answer of function `f` and therefore does execute it only once

    :param f:
    :return:
    """
    function_cache = f.__memoisation_cache__ = {}

    @wraps(f)
    def wrapper(*args, **kwargs):
        key = tuple(args), frozenset(kwargs)
        try:
            return function_cache[key]
        except KeyError:
            pass

        ans = f(*args, **kwargs)
        function_cache[key] = ans
        return ans

    return wrapper
