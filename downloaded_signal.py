from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import os
import luigi
import shutil
from chromosomes import Chromosomes
from downloader import fetch
from task import Task


class DownloadedSignal(Task):

    cell_type = luigi.Parameter()
    track = luigi.Parameter()
    genome_version = luigi.Parameter()

    chromosomes = Chromosomes.collection

    def url(self):
        if self.genome_version != 'hg19':
            raise ValueError('Unsupported genome version {!r}'.format(self.genome_version))

        template = 'http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/pval/' \
                   '{cell_type}-{track}.pval.signal.bigwig'
        return template.format(cell_type=self.cell_type, track=self.track)

    @property
    def parameters(self):
        return [self.cell_type, self.track, self.genome_version, self.chromosomes]

    def requires(self):
        return Chromosomes(genome_version=self.genome_version, collection=self.chromosomes)

    @property
    def _extension(self):
        return 'bdg.gz'

    def run(self):
        from command_line_applications.ucsc_suite import bigWigToBedGraph
        if self.chromosomes in ['male', 'all', 'chrY']:
            raise ValueError('Unsupported chromosomes: {!r}'.foirmat(self.chromosomes))

        logger = self.logger()
        url = self.url()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        chromosome_sizes = self.input().load()

        with self.temporary_directory():
            logger.debug('Fetching: {}'.format(url))
            tmp_file = 'download.bigwig'
            with open(tmp_file, 'w') as f:
                fetch(url, f)

            logger.debug('Converting to bedgraph')
            tmp_bedgaph = 'download.bedgraph'
            bigWigToBedGraph(tmp_file, tmp_bedgaph)

            filtered_output = 'filtered.gz'
            with gzip.GzipFile(filtered_output, 'w') as out_:
                with open(tmp_bedgaph, 'r') as input_:
                    for row in input_:
                        chrom = row.split('\t')[0]

                        if chrom in chromosome_sizes:
                            out_.write(row)

            logger.debug('Moving')
            shutil.move(filtered_output, output_abspath)


if __name__ == '__main__':
    import logging
    DownloadedSignal.logger().setLevel(logging.DEBUG)
    logging.basicConfig()

    import luigi
    luigi.run(main_task_cls=DownloadedSignal)