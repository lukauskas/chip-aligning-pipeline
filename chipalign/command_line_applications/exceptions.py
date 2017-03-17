import sh
import logging

import sys

import traceback


def log_sh_exceptions(func):
    """
    Decorator around sh functions to log their exceptions in full.

    :param func:
    :return:
    """

    def _f(*args, **kwargs):
        logger = logging.getLogger(
            'chipalign.command_line_applications.exceptions.log_sh_exceptions')
        try:
            func(*args, **kwargs)
        except sh.ErrorReturnCode as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exc_traceback = ''.join(traceback.format_tb(exc_traceback))

            logger.error('Got error return code {!r} when running sh command'.format(e),
                         extra=dict(exc_type=exc_type,
                                    exc_value=exc_value,
                                    exc_traceback=exc_traceback,
                                    full_cmd=e.full_cmd,
                                    stdout=e.stdout,
                                    stderr=e.stderr))

            raise

    return _f
