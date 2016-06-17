from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip

import luigi
import shutil

from chipalign.core.task import Task
from chipalign.core.util import ensure_directory_exists_for_file, temporary_file, \
    autocleaning_pybedtools
from chipalign.genome.blacklist import BlacklistedRegions, remove_blacklisted_regions


class NonOverlappingBins(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()

    remove_blacklisted = luigi.BooleanParameter(default=True)

    @property
    def task_class_friendly_name(self):
        return 'NOB'

    @property
    def _extension(self):
        return 'bed.gz'

    @property
    def _blacklisted_regions(self):
        return BlacklistedRegions(genome_version=self.genome_version)

    def requires(self):
        if self.remove_blacklisted:
            return self._blacklisted_regions
        else:
            return []

    def run(self):
        ensure_directory_exists_for_file(self.output().path)
        with autocleaning_pybedtools() as pybedtools:
            windows = pybedtools.BedTool().window_maker(w=self.window_size,
                                                        g=pybedtools.chromsizes(self.genome_version))
            windows = windows.sort()

            if self.remove_blacklisted:
                blacklist = pybedtools.BedTool(self._blacklisted_regions.output().path)
                windows = remove_blacklisted_regions(windows, blacklist)
                del blacklist

            with temporary_file() as tmp_filename:
                windows.saveas(tmp_filename)

                with temporary_file() as gzip_tmp:
                    with open(tmp_filename, 'r') as input_file:
                        with gzip.GzipFile(gzip_tmp, 'w') as gzipped_file:
                                gzipped_file.writelines(input_file)

                    shutil.move(gzip_tmp, self.output().path)


if __name__ == '__main__':
    luigi.run(main_task_cls=NonOverlappingBins)
