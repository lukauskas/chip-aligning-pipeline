from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import shutil

import luigi

from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import temporary_file, autocleaning_pybedtools, capture_output
from chipalign.database.core.downloaded_signal_base import DownloadedSignalBase


def encode_download_url(accession, file_type):
    url = 'https://www.encodeproject.org/files/{accession}/@@download/{accession}.{file_type}'.format(
        accession=accession,
        file_type=file_type)
    return url

def fetch_from_encode(accession, file_type, output):
    logger = logging.getLogger('chipalign.database.encode.download.fetch_from_encode')
    url = encode_download_url(accession, file_type)
    try:
        fetch(url, output)
    except Exception as e:
        logger.error('Error while downloading {}:\n{!r}'.format(url, e))
        raise


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

    def _download_file_and_verify(self):

        with temporary_file() as tf:
            with open(tf, 'wb') as handle:
                fetch_from_encode(self.accession, 'bam', handle)

            # Verify integrity of BAM file
            with autocleaning_pybedtools() as pybedtools:
                with capture_output() as output:
                    bed = pybedtools.BedTool(tf).bam_to_bed()

            if output['stdout'] or output['stderr']:
                joint_output = ''.join([output['stdout'], output['stderr']])
                raise Exception(
                    'Not empty output while loading BAM file: {}'.format(joint_output))
            else:
                # Otherwise integrity has been verified
                shutil.move(tf, self.output().path)

    def _run(self):
        self.ensure_output_directory_exists()

        attempt_number = 0
        n_attempts = 2

        while attempt_number < n_attempts:
            attempt_number += 1
            try:
                self._download_file_and_verify()
            except Exception as e:
                if attempt_number == n_attempts:
                    logging.error('Got {} while verifying the file, '
                                  'exhausted number of attempts'.format(e))
                    raise
                else:
                    logging.warning('Got {} while verifying the file, '
                                    'attempting to redownload'.format(e))
                    continue
            else:
                # No error - no need to do second attempt
                break

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
        return encode_download_url(self.accession, 'bigWig')
