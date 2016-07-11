from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from cStringIO import StringIO
from contextlib import contextmanager
import datetime
from itertools import imap
import os.path
import tempfile
import os
import shutil
import logging
import sys

_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR = 'CHIPALIGN_OUTPUT_DIRECTORY'
_CLEANUP_ON_EXCEPTION_DEFAULT = 'CHIPALIGN_NO_CLEANUP' not in os.environ

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
        os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR] = os.path.abspath(get_config()['output_directory'])
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
def timed_segment(message, logger=None):
    if logger is None:
        logger = logging.getLogger('chipalign.core.util.timed_segment')

    logger.info('Starting {}'.format(message))

    start_time = datetime.datetime.now()
    yield
    end_time = datetime.datetime.now()
    diff = (end_time-start_time)

    total_seconds = diff.total_seconds()

    logger.info('Finished {}. Took {:.2f}s'.format(message,
                                                   total_seconds),
                extra=dict(duration=total_seconds))

@contextmanager
def temporary_directory(logger=None, cleanup_on_exception=_CLEANUP_ON_EXCEPTION_DEFAULT, **kwargs):
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
    # Ensure this directory exists
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
    except BaseException as main_exception:
        os.chdir(current_working_directory)
        if cleanup_on_exception:
            # If exception, and cleanup_on_exception is set -- remove directory
            logger.debug('Removing {} as cleanup_on_exception is set'.format(temp_dir))

            try:
                shutil.rmtree(temp_dir)
            except OSError as e:
                if os.path.isdir(temp_dir):
                    logger.error('Error while removing {}: got {!r}'.format(temp_dir, e))
                    # do not re-raise as not to hide main exception
                else:
                    logger.warning(
                        'Tried to remove {}, but it was already deleted'.format(temp_dir))
        else:
            logger.debug('Not removing {} as cleanup_on_exception is false'.format(temp_dir))
        raise main_exception
    else:
        os.chdir(current_working_directory)
        # No exception case - remove always
        logger.debug('Removing {}'.format(temp_dir))
        try:
            shutil.rmtree(temp_dir)
        except OSError as e:
            if os.path.isdir(temp_dir):
                logger.error('Error while removing {}: got {!r}'.format(temp_dir, e))
                raise
            else:
                logger.warning('Tried to remove {}, but it was already deleted'.format(temp_dir))

@contextmanager
def temporary_file(logger=None, cleanup_on_exception=_CLEANUP_ON_EXCEPTION_DEFAULT, **kwargs):
    prefix = kwargs.pop('prefix', 'tmp.chipalign')
    # Default to storing the tmp files in _OUTPUT_DIR/.tmp/ directory
    dir_ = kwargs.pop('dir', os.path.join(output_dir(), '.tmp'))

    if not os.path.isdir(dir_):
        os.makedirs(dir_)

    __, temp_file = tempfile.mkstemp(prefix=prefix, dir=dir_, **kwargs)
    if logger is None:
        logger = logging.getLogger('chipalign.core.util.temporary_file')

    try:
        logger.debug('Working with temporary file: {}'.format(temp_file))
        yield temp_file
    except BaseException as main_exception:
        if cleanup_on_exception:
            # If exception, and cleanup_on_exception is set -- remove file
            logger.debug('Removing {} as cleanup_on_exception is set'.format(temp_file))
            try:
                os.unlink(temp_file)
            except OSError as e:
                if os.path.isfile(temp_file):
                    logger.error('Error while removing {}, got {!r}'.format(temp_file, e))
                    # Do not re-raise as not to hide the main exception
        else:
            logger.debug('Not removing {} as cleanup_on_exception is false'.format(temp_file))

        raise main_exception

    # If we are here, no exception occurred
    logger.debug('Removing {}'.format(temp_file))
    try:
        os.unlink(temp_file)
    except OSError:
        if os.path.isfile(temp_file):
            raise


@contextmanager
def autocleaning_pybedtools():
    """
    Returns a pybedtools instance that will clean-up after the context is over.
    Unfortunately, this is the only way for pybedtools to work with luigi and not cause
    memory leaks
    """
    import pybedtools
    if '_SKIP_PYBEDTOOLS_AUTOCLEANING' in os.environ:
        yield pybedtools
        return

    if len(pybedtools.BedTool.TEMPFILES) != 0:
        raise Exception('pybedtools.BedTool.TEMPFILES not empty on context entry. '
                        'Maybe you\'re nesting `autocleaning_pybedtools` contexts?')

    logger = logging.getLogger('chipalign.core.util.autocleaning_pybedtools')
    dir_ = os.path.abspath(os.path.join(output_dir(), '.tmp/.pybedtools/'))
    if not os.path.isdir(dir_):
        try:
            os.makedirs(dir_)
        except OSError:
            # Sometimes due to race conditions we will try to create the dir multiple times
            # Detect such case and ignore the error if it happens, else raise it
            if not os.path.isdir(dir_):
                raise
    pybedtools.set_tempdir(dir_)

    try:
        yield pybedtools
    finally:
        logger.debug('Cleaning up {:,} pybedtools files'.format(len(pybedtools.BedTool.TEMPFILES)))
        pybedtools.cleanup()


@contextmanager
def capture_output(stdout=True, stderr=True):
    """
    Context manager that captures standard output and error streams
    :param stdout: boolean, whether to capture stdout
    :param stderr: boolean, whether to capture stderr
    :return:
    """
    # implementation inspired by
    # https://stackoverflow.com/questions/5136611/capture-stdout-from-a-script-in-python
    old_stdout, old_stderr = sys.stdout, sys.stderr

    new_outputs = {}
    try:
        if stdout:
            new_outputs['stdout'] = StringIO()
            sys.stdout = new_outputs['stdout']
        if stderr:
            new_outputs['stderr'] = StringIO()
            sys.stderr = new_outputs['stderr']

        yield new_outputs
    finally:
        if stdout:
            sys.stdout = old_stdout
        if stderr:
            sys.stderr = old_stderr

        for key, stream in new_outputs.items():
            new_outputs[key] = stream.getvalue()

def fast_bedtool_from_iterable(iterable, pybedtools):
    """
    Creates a bedtool object from provided iterable in a fast way.
    Particularly it first creates the string representation and then creates BedTool object from string
    This seems to be faster than compiling it from list

    :param iterable: Iterable to convert to bedtool
    :param pybedtools: a pybedtools instance. See :func:`~chipalign.core.util.autocleaning_pybedtools`
    :return: BedTool object
    :rtype: pybedtools.BedTool
    """
    str_repr = '\n'.join(imap(str, iterable))
    return pybedtools.BedTool(str_repr, from_string=True)


def file_modification_time(path):
    return datetime.datetime.fromtimestamp(os.path.getmtime(path))