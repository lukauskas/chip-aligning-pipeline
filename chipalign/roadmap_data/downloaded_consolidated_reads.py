from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import luigi
from chipalign.core.downloader import fetch
from chipalign.core.task import Task


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

        with self.temporary_directory():
            logger.debug('Fetching: {}'.format(url))
            tmp_file = 'download.gz'
            with open(tmp_file, 'w') as f:
                fetch(url, f)

            logger.debug('Moving')
            shutil.move(tmp_file, output_abspath)