from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap
import os
import shutil
import pybedtools
import luigi

from chipalign.core.downloader import fetch
from chipalign.core.task import Task


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

        if self.genome_version in self.DOWNLOADABLE_BLACKLISTS:
            url = self.DOWNLOADABLE_BLACKLISTS[self.genome_version]
            logger.debug('Downloading the blacklist directly from {}'.format(url))
            output_abspath = os.path.abspath(self.output().path)
            with self.temporary_directory():
                tmp_file = 'download.gz'

                with open(tmp_file, 'w') as f:
                    fetch(url, f)
                shutil.move(tmp_file, output_abspath)
        else:
          raise Exception('No blacklist for genome version {!r} available'.format(self.genome_version))

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

        blacklist = pybedtools.BedTool(self._blacklist_task.output().path)
        input_ = pybedtools.BedTool(self.input_task._filename().path)

        difference = input_.intersect(blacklist, v=True)
        try:
            with self.output().open('w') as f:
                f.writelines(imap(str, difference))
        finally:
            difference_fn = difference.fn
            del difference
            try:
                os.unlink(difference_fn)
            except OSError:
                if os.path.isfile(difference_fn):
                    raise
    @property
    def task_class_friendly_name(self):
        return 'WL'


