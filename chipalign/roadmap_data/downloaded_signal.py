from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import re
import shutil

import requests

from chipalign.core.task import Task
from chipalign.genome.chromosomes import Chromosomes
from chipalign.core.downloader import fetch
from chipalign.core.file_formats.yaml_file import YamlFile
import luigi

class DownloadedSignal(Task):

    cell_type = luigi.Parameter()
    track = luigi.Parameter()
    genome_version = luigi.Parameter()

    chromosomes = Chromosomes.collection

    def url(self):
        return self.downloadable_signal_task.output().load()[self.track]

    @property
    def parameters(self):
        return [self.cell_type, self.track, self.genome_version, self.chromosomes]

    @property
    def chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version, collection=self.chromosomes)

    @property
    def downloadable_signal_task(self):
        return DownloadableSignalTracks(genome_version=self.genome_version, cell_type=self.cell_type)

    def requires(self):
        return [self.chromosomes_task, self.downloadable_signal_task]

    @property
    def _extension(self):
        return 'bdg.gz'

    def run(self):
        from chipalign.command_line_applications.ucsc_suite import bigWigToBedGraph
        from chipalign.command_line_applications.common import sort
        from chipalign.command_line_applications.archiving import gzip as cmd_line_gzip

        if self.chromosomes in ['male', 'all', 'chrY']:
            raise ValueError('Unsupported chromosomes: {!r}'.foirmat(self.chromosomes))

        logger = self.logger()
        url = self.url()

        output_abspath = os.path.abspath(self.output().path)
        self.ensure_output_directory_exists()

        chromosome_sizes = self.chromosomes_task.output().load()

        with self.temporary_directory():
            logger.info('Fetching: {}'.format(url))
            tmp_file = 'download.bigwig'
            with open(tmp_file, 'w') as f:
                fetch(url, f)

            logger.info('Converting to bedgraph')
            tmp_bedgaph = 'download.bedgraph'
            bigWigToBedGraph(tmp_file, tmp_bedgaph)

            logger.info('Filtering the bedgraph')
            filtered_bedgraph = 'filtered.bedgraph'
            with open(filtered_bedgraph, 'w') as out_:
                with open(tmp_bedgaph, 'r') as input_:
                    for row in input_:
                        chrom, __, __ = row.partition('\t')

                        if chrom in chromosome_sizes:
                            out_.write(row)

            logger.info('Sorting')
            filtered_sorted_bedgraph = 'filtered.sorted.bedgraph'
            # GNU sort should be faster, use less memory and generally more stable than pybedtools
            sort(filtered_bedgraph, '-k1,1', '-k2,2n', '-k3,3n', '-k5,5n',
                 '-o', filtered_sorted_bedgraph)

            # Free up some /tmp/ space
            os.unlink(filtered_bedgraph)

            logger.info('Gzipping')
            cmd_line_gzip('-9', filtered_sorted_bedgraph)
            filtered_and_sorted = 'filtered.sorted.bedgraph.gz'

            logger.info('Moving')
            shutil.move(filtered_and_sorted, output_abspath)


class DownloadableSignalTracks(Task):

    cell_type = luigi.Parameter()
    genome_version = luigi.Parameter()

    @property
    def parameters(self):
        return [self.cell_type, self.genome_version]

    def url(self):
        if self.genome_version == 'hg19':
            return 'http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/pval/'
        else:
            raise ValueError('Unsupported genome version {!r}'.format(self.genome_version))

    def output(self):
        return YamlFile(super(DownloadableSignalTracks, self).output().path)

    @property
    def _extension(self):
        return 'yml'

    def run(self):
        url = self.url()
        response = requests.get(url)
        response.raise_for_status()

        hrefs = re.findall('href="([^"]+)"', response.text)
        bigwigs = filter(lambda x: x.endswith('bigwig'), hrefs)

        urls_for_cell_type = {}

        for filename in bigwigs:
            match = re.match('(?P<cell_type>\w+)-(?P<track>.*?).pval.signal.bigwig', filename)
            if match is None:
                raise Exception('Could not parse {!r}'.format(filename))

            if match.group('cell_type') == self.cell_type:
                full_url = os.path.join(url, filename)
                urls_for_cell_type[match.group('track')] = full_url

        if not urls_for_cell_type:
            raise Exception('No URLs for cell type {!r} have been recovered'.format(self.cell_type))

        self.output().dump(urls_for_cell_type)
