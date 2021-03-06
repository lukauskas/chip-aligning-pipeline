from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import luigi
from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import temporary_file


class DownloadedConsolidatedReads(Task):
    """
    Downloads consolidated reads from `ROADMAP`_. This task is designed to be a
    drop-in replacement for :class:`~chipalign.alignment.consolidation.ConsolidatedReads` task.

    :param cell_type: cell_type (using ROADMAP naming scheme)
    :param track: track name (again, using ROADMAP naming)
    :param genome_version: genome version to use

    .. _ROADMAP: http://egg2.wustl.edu/roadmap/web_portal/processed_data.html#ChipSeq_DNaseSeq
    """

    cell_type = luigi.Parameter()
    track = luigi.Parameter()
    genome_version = luigi.Parameter()

    @property
    def task_class_friendly_name(self):
        return 'DConsolidatedReads'

    def url(self):
        if self.genome_version != 'hg19':
            raise ValueError('Unsupported genome version {!r}'.format(self.genome_version))

        template = 'http://egg2.wustl.edu/roadmap/data/byFileType/alignments/consolidated/' \
                   '{cell_type}-{track}.tagAlign.gz'
        return template.format(cell_type=self.cell_type, track=self.track)

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def _run(self):
        logger = self.logger()
        url = self.url()

        self.ensure_output_directory_exists()

        with temporary_file() as tf:
            logger.info('Fetching: {} to {}'.format(url, tf))
            with open(tf, 'wb') as f:
                fetch(url, f)

            # Essentially this re-gzips the downloaded reads output
            # without this, pybedtools fails to read it for whatever reason
            # see the appropriate test
            logger.info('Saving output')
            with gzip.GzipFile(tf) as in_:
                with self.output().open('w') as out_:
                    out_.writelines(in_)