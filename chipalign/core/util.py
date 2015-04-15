from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from contextlib import contextmanager
from functools import wraps
import tempfile
import os
import shutil
import logging

_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR = 'CHIPALIGN_OUTPUT_DIRECTORY'
def config_from_file():
    import yaml
    with open('chipalign.yml') as f:
        config = yaml.safe_load(f)
        return config


def get_config():
    return config_from_file()

def output_dir():

    try:
        return os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR]
    except KeyError:
        os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR] = get_config()['output_directory']
        return os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR]

def ensure_directory_exists_for_file(filename):
    """
    Ensures that the directory for filename exists. Creates the directory if it doesn't.

    :param filename: filename to ensure the directory is created for
    """
    dir_ = os.path.dirname(filename)
    try:
        os.makedirs(dir_)
    except OSError:
        if not os.path.isdir(dir_):
            raise

@contextmanager
def temporary_directory(logger=None, cleanup_on_exception=False, **kwargs):
    """
    A context manager to change the current working directory to a temporary directory,
    and change it back, removing the temporary directory afterwards

    :param logger: if set to non none, the code will call logger.debug() to write which directory it is working in
    :type logger: :class:`logging.Logger`
    :param cleanup_on_exception: If set to true, the temporary directory will be removed even when exception occurs
    :type cleanup_on_exception: bool
    :param kwargs: kwargs to pass to :func:`tempfile.mkdtemp`
    :return:
    """
    current_working_directory = os.getcwd()

    # Default to storing the tmp files in _OUTPUT_DIR/.tmp/ directory
    dir_ = kwargs.pop('dir', os.path.join(output_dir(), '.tmp'))
    # Encsure this directory exists
    try:
        os.makedirs(dir_)
    except OSError:
        if not os.path.isdir(dir_):
            raise

    temp_dir = tempfile.mkdtemp(dir=dir_, **kwargs)

    if logger is None:
        logger = logging.getLogger('chipalign.core.util.temporary_directory')

    try:
        logger.debug('Working on: {}'.format(temp_dir))
        os.chdir(temp_dir)
        yield temp_dir
    except:
        if cleanup_on_exception:
            # If exception, and cleanup_on_exception is set -- remove directory
            logger.debug('Removing {} as cleanup_on_exception is set'.format(temp_dir))
            shutil.rmtree(temp_dir)
        raise
    finally:
        os.chdir(current_working_directory)

    # No exception case - remove always
    logger.debug('Removing {}'.format(temp_dir))
    shutil.rmtree(temp_dir)

@contextmanager
def temporary_file(logger=None, cleanup_on_exception=False, **kwargs):

    __, temp_file = tempfile.mkstemp(**kwargs)
    if logger is None:
        logger = logging.getLogger('chipalign.core.util.temporary_file')

    try:
        logger.debug('Working with temporary file: {}'.format(temp_file))
        yield temp_file
    except:
        if cleanup_on_exception:
            # If exception, and cleanup_on_exception is set -- remove file
            logger.debug('Removing {} as cleanup_on_exception is set'.format(temp_file))
            try:
                os.unlink(temp_file)
            except OSError:
                if os.path.isfile(temp_file):
                    raise

    # If we are here, no exception occurred
    logger.debug('Removing {}'.format(temp_file))
    try:
        os.unlink(temp_file)
    except OSError:
        if os.path.isfile(temp_file):
            raise

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
