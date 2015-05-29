from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip

import luigi
import pybedtools
import shutil

from chipalign.core.task import Task
from chipalign.core.util import ensure_directory_exists_for_file, temporary_file
from chipalign.genome.blacklist import BlacklistedRegions, remove_blacklisted_regions


class NonOverlappingWindows(Task):

    genome_version = luigi.Parameter()
    window_size = luigi.IntParameter()

    remove_blacklisted = luigi.BooleanParameter(default=True)

    @property
    def task_class_friendly_name(self):
        return 'NOW'

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
        windows = None
        try:
            windows = pybedtools.BedTool().window_maker(w=self.window_size,
                                                        g=pybedtools.chromsizes(self.genome_version))
            windows = windows.sort()

            if self.remove_blacklisted:
                blacklist = pybedtools.BedTool(self._blacklisted_regions.output().path)
                windows = remove_blacklisted_regions(windows, blacklist)
                del blacklist

            with temporary_file(cleanup_on_exception=True) as tmp_filename:
                windows.saveas(tmp_filename)

                with temporary_file(cleanup_on_exception=True) as gzip_tmp:
                    with open(tmp_filename, 'r') as input_file:
                        with gzip.GzipFile(gzip_tmp, 'w') as gzipped_file:
                                gzipped_file.writelines(input_file)

                    shutil.move(gzip_tmp, self.output().path)

        finally:
            if windows:
                windows.delete_temporary_history(ask=False)

if __name__ == '__main__':
    luigi.run(main_task_cls=NonOverlappingWindows)