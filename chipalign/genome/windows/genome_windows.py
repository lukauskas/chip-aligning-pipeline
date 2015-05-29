from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import tempfile

import luigi
import pybedtools

from chipalign.core.task import Task
from chipalign.core.util import ensure_directory_exists_for_file


class NonOverlappingWindows(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()

    @property
    def task_class_friendly_name(self):
        return 'NOW'

    @property
    def _extension(self):
        return 'bed.gz'

    def run(self):
        ensure_directory_exists_for_file(self.output().path)
        windows = None
        try:
            windows = pybedtools.BedTool().window_maker(w=self.window_size,
                                                        g=pybedtools.chromsizes(self.genome_version))
            windows = windows.sort()

            __, tmp_filename = tempfile.mkstemp()
            windows.saveas(tmp_filename)

            try:
                with open(tmp_filename, 'r') as input_file:
                    with gzip.GzipFile(self.output().path, 'w') as gzipped_file:
                            gzipped_file.writelines(input_file)
            finally:
                os.unlink(tmp_filename)
        finally:
            if windows:
                windows.delete_temporary_history(ask=False)

if __name__ == '__main__':
    luigi.run(main_task_cls=NonOverlappingWindows)