from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
from task import Task
from downloader import fetch

class GenomeIndex(Task):
    """
    Downloads/creates bowtie2 index for the specified genome version

    """
    genome_version = luigi.Parameter()

    _DOWNLOADABLE_INDICES = {'hg18': 'ftp://ftp.ccb.jhu.edu/pub/data/bowtie2_indexes/hg18.zip',
                             'hg19': 'ftp://ftp.ccb.jhu.edu/pub/data/bowtie2_indexes/hg19.zip'}

    _GENOME_SEQUENCES = {'hg39'}

    @property
    def _parameters(self):
        return [self.genome_version]

    def run(self):
        if self.genome_version in self._DOWNLOADABLE_INDICES:
            with self.output().open('w') as output_file:
                fetch(self._DOWNLOADABLE_INDICES[self.genome_version], output_file)
        else:
            raise ValueError('Unsupported genome version: {0!r}'.format(self.genome_version))
