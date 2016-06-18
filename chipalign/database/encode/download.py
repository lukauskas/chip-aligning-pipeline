from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi

from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.database.core.downloaded_signal_base import DownloadedSignalBase


def _encode_download_url(accession, file_type):
    url = 'https://www.encodeproject.org/files/{accession}/@@download/{accession}.{file_type}'.format(
        accession=accession,
        file_type=file_type)
    return url


def _fetch_from_encode(accession, file_type, output):
    fetch(_encode_download_url(accession, file_type), output)


class EncodeRawReads(Task):
    """
    Downloads the short reads from `ENCODE`_.
    Should be a drop-in replacement for :class:`~chipalign.sequence.srr` task.
    .. _ENCODE: https://www.encodeproject.org/
    """

    @property
    def _extension(self):
        return 'fastq.gz'

    accession = luigi.Parameter()

    def run(self):
        with self.output().open('w') as output:
            _fetch_from_encode(self.accession, 'fastq', output)


class EncodeAlignedReads(Task):
    """
    Downloads aligned reads from `ENCODE`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.alignment.consolidation.AlignedSRR` task.

    :param accession: roadmap ACCESSION to use, e.g. 'ENCFF000VTE'

    .. seealso:: :class:`~chipalign.database.roadmap.downloaded_reads.RoadmapAlignedReads`

    .. _ENCODE: https://www.encodeproject.org/
    """
    accession = luigi.Parameter()

    @property
    def _extension(self):
        return 'bam'

    def run(self):
        with self.output().open('w') as output:
            _fetch_from_encode(self.accession, 'bam', output)

    def bam_output(self):
        return self.output()


class EncodeDownloadedSignal(DownloadedSignalBase):
    """
    Downloads signal tracks from from `ENCODE`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.signal.signal.Signal` task.

   :param accession: Accession number for 'signal p-value' track, i.e. 'ENCFF077EXO'

    .. _ENCODE: https://www.encodeproject.org/
    """

    accession = luigi.Parameter()

    def url(self):
        return _encode_download_url(self.accession, 'bigWig')
