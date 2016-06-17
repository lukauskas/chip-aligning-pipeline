from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import luigi

from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import temporary_file, autocleaning_pybedtools


class BlacklistedRegions(Task):
    """
        Downloads a list of blacklisted regions for a particular genome version
    """
    DOWNLOADABLE_BLACKLISTS = {'hg19': 'http://hgdownload.cse.ucsc.edu/goldenPath/hg19/encodeDCC/'
                                       'wgEncodeMapability/wgEncodeDacMapabilityConsensusExcludable.bed.gz'}

    genome_version = luigi.Parameter()

    @property
    def parameters(self):
        return [self.genome_version]

    def run(self):
        logger = self.logger()

        if self.genome_version not in self.DOWNLOADABLE_BLACKLISTS:
            raise Exception('No blacklist for genome version {!r} available'.format(self.genome_version))

        url = self.DOWNLOADABLE_BLACKLISTS[self.genome_version]
        logger.debug('Downloading the blacklist directly from {}'.format(url))
        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()
        with self.temporary_directory():
            tmp_file = 'download.gz'

            with open(tmp_file, 'w') as f:
                fetch(url, f)

            shutil.move(tmp_file, output_abspath)

    @property
    def _extension(self):
        return 'bed.gz'

def remove_blacklisted_regions(input_bedtool, blacklist_bedtool):
    difference = input_bedtool.intersect(blacklist_bedtool, v=True)
    return difference

class NonBlacklisted(Task):
    genome_version = BlacklistedRegions.genome_version

    input_task = luigi.Parameter()

    @property
    def parameters(self):
        return [self.input_task.task_class_friendly_name] + self.input_task.parameters + [self.genome_version]

    @property
    def _blacklist_task(self):
        return BlacklistedRegions(genome_version=self.genome_version)

    def requires(self):
        return [self.input_task, self._blacklist_task]

    @property
    def _extension(self):
        return 'bed.gz'

    def run(self):

        with autocleaning_pybedtools() as pybedtools:
            input_ = pybedtools.BedTool(self.input_task.output().path)
            blacklist = pybedtools.BedTool(self._blacklist_task.output().path)

            with temporary_file() as temp:
                remove_blacklisted_regions(input_, blacklist).saveas(temp)

                with open(temp, 'r') as read_:
                    with self.output().open('w') as f:
                        f.writelines(read_)

    @property
    def task_class_friendly_name(self):
        return 'WL'

if __name__ == '__main__':
    luigi.run()