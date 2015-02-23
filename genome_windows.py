from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import luigi
from task import Task
import pybedtools
import tempfile

class NonOverlappingWindows(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()

    @property
    def parameters(self):
        return [self.genome_version, 'w{}'.format(self.window_size)]

    @property
    def _extension(self):
        return 'bed.gz'

    def run(self):
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

if __name__ == '__main__':
    luigi.run(main_task_cls=NonOverlappingWindows)