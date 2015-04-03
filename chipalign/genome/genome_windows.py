from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import tempfile

import luigi
import pybedtools

from chipalign.genome.chromosomes import Chromosomes
from chipalign.core.task import Task
from chipalign.core.util import ensure_directory_exists_for_file


class NonOverlappingWindows(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()

    chromosomes = Chromosomes.collection

    use_non_fully_mappable_windows = luigi.BooleanParameter(default=False)

    @property
    def task_class_friendly_name(self):
        return 'NOW'

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

    @property
    def chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version, collection=self.chromosomes)

    def requires(self):
        return self.chromosomes_task

    def _chromosomes_filter(self):
        chromosomes = self.chromosomes_task.output().load()
        return lambda x: x.chrom in chromosomes

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