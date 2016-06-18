from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi

from chipalign.core.downloader import fetch
from chipalign.core.task import Task


class EncodeAlignedReads(Task):
    """
    Downloads aligned reads from `Encode`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.alignment.consolidation.AlignedSRR` task.

    :param accession: roadmap ACCESSION to use, e.g. 'ENCFF000VTE'

    .. seealso:: :class:`~chipalign.database.roadmap.downloaded_reads.RoadmapAlignedReads`

    .. _ENCODE: https://www.encodeproject.org/
    """
    accession = luigi.Parameter()

    @property
    def _extension(self):
        return 'bam'

    @property
    def url(self):
        return 'https://www.encodeproject.org/files/{accession}/@@download/{accession}.bam'.format(accession=self.accession)

    def run(self):
        import logging

        url = self.url

        logging.info('Downloading {}'.format(url))
        with self.output().open('w') as output:
            fetch(url, output)

    def bam_output(self):
        return self.output()
