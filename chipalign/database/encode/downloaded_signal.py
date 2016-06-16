from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
from chipalign.database.core.downloaded_signal_base import DownloadedSignalBase


class EncodeDownloadedSignal(DownloadedSignalBase):
    """
    Downloads signal tracks from from `ENCODE`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.signal.signal.Signal` task.

   :param accession: Accession number for 'signal p-value' track, i.e. 'ENCFF077EXO'

    .. _ENCODE: https://www.encodeproject.org/
    """

    accession = luigi.Parameter()

    def url(self):
        return 'https://www.encodeproject.org/files/{accession}/@@download/{accession}.bigWig'.format(accession=self.accession)
