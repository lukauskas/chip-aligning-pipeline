from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
import luigi.format

from chipalign.core.util import file_modification_time

class File(luigi.File):

    @property
    def modification_time(self):
        if not self.exists():
            return None
        else:
            return file_modification_time(self.path)

class GzippedFile(File):

    def __init__(self, path=None):
        super(GzippedFile, self).__init__(path=path, format=luigi.format.Gzip)
