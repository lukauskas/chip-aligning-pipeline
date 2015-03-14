from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging

import luigi
import luigi.format

import re
import os

_OUTPUT_DIR = 'data/' # TODO: make this readable from config

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
        if self._extension.endswith('.gz'):
            return GzipOutputFile(self.__full_path)
        else:
            return luigi.File(self.__full_path)

    @classmethod
    def logger(cls):
        return logging.getLogger(cls.__name__)
