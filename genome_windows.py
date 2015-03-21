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
from util import ensure_directory_exists_for_file


class NonOverlappingWindows(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()

    chromosomes = luigi.Parameter(default='all')  # 'all', 'male', 'female'

    @property
    def parameters(self):
        params = [self.genome_version]
        if self.chromosomes != 'all':
            params.append(self.chromosomes)

        params.append('w{}'.format(self.window_size))
        return params

    @property
    def _extension(self):
        return 'bed.gz'

    def _chromosomes_filter(self):
        if self.chromosomes == 'all':
            return lambda x: True

        female_chromosomes = {'chr{}'.format(x) for x in (range(1, 23) + ['X'])}
        male_chromosomes = female_chromosomes | {'Y'}

        if self.chromosomes == 'female':
            return lambda x: x.chrom in female_chromosomes
        elif self.chromosomes == 'male':
            return lambda x: x.chrom in male_chromosomes
        else:
            raise ValueError('Unsupported chromosomes type: {!r}'.format(self.chromosomes))

    def run(self):
        ensure_directory_exists_for_file(self.output().path)
        chromosomes_filter = self._chromosomes_filter()

        try:
            windows = pybedtools.BedTool().window_maker(w=self.window_size,
                                                        g=pybedtools.chromsizes(self.genome_version))

            windows = pybedtools.BedTool(filter(chromosomes_filter, windows))

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
            pybedtools.cleanup()

if __name__ == '__main__':
    luigi.run(main_task_cls=NonOverlappingWindows)