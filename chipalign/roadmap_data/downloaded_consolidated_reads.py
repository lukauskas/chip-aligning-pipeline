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
from chipalign.genome.chromosomes import Chromosomes


class DownloadedConsolidatedReads(Task):

    cell_type = luigi.Parameter()
    track = luigi.Parameter()
    genome_version = luigi.Parameter()

    chromosomes = Chromosomes.collection

    @property
    def task_class_friendly_name(self):
        return 'DConsolidatedReads'

    def url(self):
        if self.genome_version != 'hg19':
            raise ValueError('Unsupported genome version {!r}'.format(self.genome_version))

        template = 'http://egg2.wustl.edu/roadmap/data/byFileType/alignments/consolidated/{cell_type}-{track}.tagAlign.gz'
        return template.format(cell_type=self.cell_type, track=self.track)

    @property
    def parameters(self):
        return [self.cell_type, self.track, self.genome_version, self.chromosomes]

    def requires(self):
        return Chromosomes(genome_version=self.genome_version, collection=self.chromosomes)

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def run(self):
        logger = self.logger()
        url = self.url()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        chromosome_sizes = self.input().load()

        with self.temporary_directory():
            logger.debug('Fetching: {}'.format(url))
            tmp_file = 'download.gz'
            with open(tmp_file, 'w') as f:
                fetch(url, f)

            logger.debug('Filtering data')
            filtered_file = 'filtered.gz'
            with gzip.GzipFile(filtered_file, 'w') as out_:
                with gzip.GzipFile(tmp_file, 'r') as input_:
                    for row in input_:
                        chrom = row.split('\t')[0]

                        if chrom in chromosome_sizes:
                            out_.write(row)

            logger.debug('Moving')
            shutil.move(filtered_file, output_abspath)