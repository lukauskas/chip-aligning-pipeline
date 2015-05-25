from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
import os.path
import datetime

class File(luigi.File):

    @property
    def modification_time(self):
        if not self.exists():
            return None
        else:
            return datetime.datetime.fromtimestamp(os.path.getmtime(self.path))