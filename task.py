from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
import re
import os

_OUTPUT_DIR = 'data/' # TODO: make this readable from config

def _file_safe_string(value):
    value = unicode(value)
    value = re.sub('[^a-zA-Z0-9]', '_', value)
    value = re.sub('__+', '_', value)
    return value

class Task(luigi.Task):

    @property
    def parameters(self):
        return []

    @property
    def _basename(self):
        class_name = self.__class__.__name__
        parameters = self.parameters

        basename_components = [class_name] + parameters
        basename_components = map(_file_safe_string, basename_components)
        basename = u'.'.join(basename_components)
        return basename

    @property
    def _extension(self):
        return u''

    @property
    def __full_path(self):
        return os.path.join(_OUTPUT_DIR, u'.'.join([self._basename, self._extension]))

    def output(self):
        return luigi.File(self.__full_path)

