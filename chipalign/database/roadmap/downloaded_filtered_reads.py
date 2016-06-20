from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import luigi
from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import temporary_file

import os

class RoadmapDownloadedFilteredReads(Task):
    """
    Downloads filtered reads from `ROADMAP`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.alignment.filtering.FilteredReads` task.

    :param uri: uri of the read to download, as reported by
    :func:`~chipalign.database.roadmap.settings.downloadable_unconsolidated_reads`

    .. _ROADMAP: http://egg2.wustl.edu/roadmap/web_portal/processed_data.html#ChipSeq_DNaseSeq

    .. seealso:: :func:`~chipalign.database.roadmap.settings.downloadable_unconsolidated_reads`
    """

    genome_version = luigi.Parameter(significant=False)  # Genome version (ConsolidatedReads Task requires it)
    uri = luigi.Parameter()  # URI of the read to download

    @property
    def task_class_friendly_name(self):
        return 'RDFR'

    @property
    def parameters(self):
        # Just return the basename
        basename = os.path.basename(self.uri).replace('.filt.tagAlign.gz', '')
        return [basename]

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def run(self):
        logger = self.logger()
        url = self.uri

        self.ensure_output_directory_exists()

        with temporary_file() as tf:
            logger.info('Fetching: {} to {}'.format(url, tf))
            with open(tf, 'w') as f:
                fetch(url, f)

            # Essentially this re-gzips the downloaded reads output
            # without this, pybedtools fails to read it for whatever reason
            # see the appropriate test
            logger.info('Saving output')
            with gzip.GzipFile(tf) as in_:
                with self.output().open('w') as out_:
                    out_.writelines(in_)