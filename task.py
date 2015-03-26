from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging

import luigi
import luigi.format

import re
import os
from util import temporary_directory, ensure_directory_exists_for_file, _OUTPUT_DIR

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
    def __full_path(self):
        class_name = self.__class__.__name__
        return os.path.join(_OUTPUT_DIR, class_name,
                            u'.'.join([self._basename, self._extension]))

    def output(self):
        path = self.__full_path
        if self._extension.endswith('.gz'):
            return GzipOutputFile(path)
        else:
            return luigi.File(path)

    @classmethod
    def logger(cls):
        return logging.getLogger('task.{}'.format(cls.__name__))

    def temporary_directory(self, **kwargs):
        prefix = kwargs.pop('prefix', 'tmp-{}'.format(self.__class__.__name__))
        cleanup_on_exception = kwargs.pop('cleanup_on_exception', False)
        return temporary_directory(logger=self.logger(), prefix=prefix, cleanup_on_exception=cleanup_on_exception, **kwargs)

    def ensure_output_directory_exists(self):
        ensure_directory_exists_for_file(os.path.abspath(self.output().path))

class MetaTask(luigi.Task):

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