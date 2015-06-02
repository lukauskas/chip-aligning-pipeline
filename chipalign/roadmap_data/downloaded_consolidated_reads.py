from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import shutil
import luigi
from chipalign.core.downloader import fetch
from chipalign.core.task import Task
from chipalign.core.util import temporary_file


class DownloadedConsolidatedReads(Task):

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

    def run(self):
        logger = self.logger()
        url = self.url()

        output_abspath = os.path.abspath(self.output().path)
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