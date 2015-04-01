from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import re
import os

import luigi
import luigi.format

from chipalign.core.util import temporary_directory, ensure_directory_exists_for_file, output_dir


def _file_safe_string(value):
    value = unicode(value)
    value = re.sub('[^a-zA-Z0-9]', '_', value)
    value = re.sub('__+', '_', value)
    return value

class GzipOutputFile(luigi.File):

    def __init__(self, path=None):
        super(GzipOutputFile, self).__init__(path=path, format=luigi.format.Gzip)


class Task(luigi.Task):

    @property
    def task_class_friendly_name(self):
        return self.__class__.__name__

    @property
    def parameters(self):
        return []

    @property
    def _basename(self):
        parameters = self.parameters

        basename_components = parameters
        basename_components = map(_file_safe_string, basename_components)
        basename = u'.'.join(basename_components)
        return basename

    @property
    def _extension(self):
        return u''

    @property
    def _output_filename(self):
        filename = u'.'.join([self._basename, self._extension])
        # Actually longer filenames are allowed, but luigi likes to append things to the end so we're being conservative
        assert len(filename) < 230, 'Filename {!r} is too long'.format(filename)
        return filename

    @property
    def __full_path(self):
        class_name = self.__class__.__name__
        return os.path.join(output_dir(), class_name,
                            self._output_filename)

    @property
    def _output_class(self):
        if self._extension.endswith('.gz'):
            return GzipOutputFile
        else:
            return luigi.File

    def output(self):
        path = self.__full_path
        return self._output_class(path)

    @classmethod
    def class_logger(cls):
        logger = logging.getLogger('task.{}'.format(cls.__name__))
        return logger

    def logger(self):
        logger = self.class_logger()

        extra = {'class': self.__class__.__name__,
                 'parameters': '.'.join(map(str, self.parameters)),
                 'output_filename': self._output_filename}

        return logging.LoggerAdapter(logger, extra)



    def temporary_directory(self, **kwargs):
        prefix = kwargs.pop('prefix', 'tmp-{}'.format(self.__class__.__name__))
        cleanup_on_exception = kwargs.pop('cleanup_on_exception', False)
        return temporary_directory(logger=self.logger(),
                                   prefix=prefix, cleanup_on_exception=cleanup_on_exception, **kwargs)

    def ensure_output_directory_exists(self):
        ensure_directory_exists_for_file(os.path.abspath(self.output().path))

class MetaTask(luigi.Task):

    @property
    def task_class_friendly_name(self):
        return self.__class__.__name__

    def complete(self):
        return self.requires().complete()

    def output(self):
        return self.requires().output()

    def requires(self):
        raise NotImplementedError

    def run(self):
        raise Exception('MetaTasks should never be run as they are completed when requires task is complete')

    @property
    def self_parameters(self):
        return []

    @property
    def parameters(self):
        return self.self_parameters + self.requires().parameters