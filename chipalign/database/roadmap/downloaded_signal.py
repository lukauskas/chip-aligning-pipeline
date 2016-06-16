from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


import luigi

from chipalign.database.roadmap.signal_tracks_list import SignalTracksList
from chipalign.database.core.downloaded_signal_base import DownloadedSignalBase


class RoadmapDownloadedSignal(DownloadedSignalBase):
    """
    Downloads signal tracks from from `ROADMAP`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.signal.signal.Signal` task.

    :param cell_type: cell_type (using ROADMAP naming scheme)
    :param track: track name (again, using ROADMAP naming)
    :param genome_version: genome version to use

    .. _ROADMAP: http://egg2.wustl.edu/roadmap/web_portal/processed_data.html
    """

    cell_type = luigi.Parameter()
    track = luigi.Parameter()
    genome_version = luigi.Parameter()

    def url(self):
        return self.downloadable_signal_task.output().load()[self.track]

    @property
    def downloadable_signal_task(self):
        return SignalTracksList(genome_version=self.genome_version, cell_type=self.cell_type)

    def requires(self):
        return [self.downloadable_signal_task]
