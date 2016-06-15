from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil

import luigi

from chipalign.core.file_formats.bedgraph import BedGraph
from chipalign.core.task import Task
from chipalign.core.downloader import fetch
from chipalign.roadmap_data.signal_tracks_list import SignalTracksList


class DownloadedSignal(Task):
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

    @property
    def task_class_friendly_name(self):
        return 'DSignal'

    def url(self):
        return self.downloadable_signal_task.output().load()[self.track]

    @property
    def downloadable_signal_task(self):
        return SignalTracksList(genome_version=self.genome_version, cell_type=self.cell_type)

    def requires(self):
        return [self.downloadable_signal_task]

    @property
    def _extension(self):
        return 'bdg.gz'

    @property
    def _output_class(self):
        return BedGraph

    def run(self):
        from chipalign.command_line_applications.ucsc_suite import bigWigToBedGraph
        from chipalign.command_line_applications.common import sort
        from chipalign.command_line_applications.archiving import gzip as cmd_line_gzip

        logger = self.logger()
        url = self.url()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        with self.temporary_directory():
            logger.info('Fetching: {}'.format(url))
            tmp_file = 'download.bigwig'
            with open(tmp_file, 'w') as f:
                fetch(url, f)

            logger.info('Converting to bedgraph')
            tmp_bedgraph = 'download.bedgraph'
            bigWigToBedGraph(tmp_file, tmp_bedgraph)

            logger.info('Sorting')
            filtered_sorted_bedgraph = 'filtered.sorted.bedgraph'
            # GNU sort should be faster, use less memory and generally more stable than pybedtools
            sort(tmp_bedgraph, '-k1,1', '-k2,2n', '-k3,3n', '-k5,5n',
                 '-o', filtered_sorted_bedgraph)

            # Free up some /tmp/ space
            os.unlink(tmp_bedgraph)

            logger.info('Gzipping')
            cmd_line_gzip('-9', filtered_sorted_bedgraph)
            filtered_and_sorted = 'filtered.sorted.bedgraph.gz'

            logger.info('Moving')
            shutil.move(filtered_and_sorted, output_abspath)
