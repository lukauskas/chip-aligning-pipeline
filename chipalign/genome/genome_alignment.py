from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
import logging
import os
import shutil

import luigi
import pybedtools

from chipalign.alignment.aligned_reads import AlignedSRR
from chipalign.alignment.filtering import FilteredReads
from chipalign.genome.chromosomes import Chromosomes
from chipalign.core.downloader import fetch
from chipalign.core.task import Task


class ConsolidatedReads(Task):

    srr_identifiers = luigi.Parameter(is_list=True)

    genome_version = FilteredReads.genome_version
    aligner = AlignedSRR.aligner

    resized_length = FilteredReads.resized_length
    filter_uniquely_mappable_for_truncated_length = FilteredReads.filter_uniquely_mappable_for_truncated_length
    remove_duplicates = FilteredReads.remove_duplicates
    sort = FilteredReads.sort

    chromosomes = Chromosomes.collection

    max_sequencing_depth = luigi.IntParameter(default=45000000)
    subsample_random_seed = luigi.IntParameter(default=0)

    @property
    def parameters(self):
        parameters = [self.genome_version, self.aligner]
        if isinstance(self.srr_identifiers, list) or isinstance(self.srr_identifiers, tuple):
            parameters.append(';'.join(self.srr_identifiers))
        else:
            raise Exception('Unexpected type of srr identifiers: {}'.format(type(self.srr_identifiers)))

        parameters += self.requires()[0].filtering_parameters  # Should be the same for all tasks
        parameters += [self.chromosomes]
        parameters += [self.max_sequencing_depth, self.subsample_random_seed]

        return parameters

    def requires(self):
        kwargs = dict(genome_version=self.genome_version,
                      resized_length=self.resized_length,
                      filter_uniquely_mappable_for_truncated_length=self.filter_uniquely_mappable_for_truncated_length,
                      remove_duplicates=self.remove_duplicates,
                      sort=self.sort,
                      chromosomes=self.chromosomes)

        tasks = []
        for srr_identifier in self.srr_identifiers:
            aligned_reads = AlignedSRR(srr_identifier=srr_identifier,
                                         genome_version=self.genome_version,
                                         aligner=self.aligner)

            tasks.append(FilteredReads(alignment_task=aligned_reads, **kwargs))

        return tasks

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def run(self):

        try:
            master_reads = []

            logger = self.logger()

            for filtered_reads in self.input():
                logger.debug('Processing {}'.format(filtered_reads.path))
                master_reads.extend(pybedtools.BedTool(filtered_reads.path))

            master_reads = pybedtools.BedTool(master_reads)
            length_of_master_reads = len(master_reads)

            logger.debug('Total {} reads'.format(length_of_master_reads))
            if length_of_master_reads > self.max_sequencing_depth:
                logger.debug('Subsampling')

                master_reads = master_reads.sample(n=self.max_sequencing_depth, seed=self.subsample_random_seed)

                if self.sort:
                    logger.debug('Sorting')
                    master_reads = master_reads.sort()

            logger.debug('Writing to file')

            with self.output().open('w') as f:
                for row in master_reads:
                    f.write(str(row))

            logger.debug('Done')

        finally:
            pybedtools.cleanup()

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

if __name__ == '__main__':
    import inspect
    import sys
    task_classes = inspect.getmembers(sys.modules[__name__],
                                      lambda x: inspect.isclass(x) and issubclass(x, Task) and x != Task)
    for __, task_class in task_classes:
        task_class.logger().setLevel(logging.DEBUG)

    logging.getLogger('MappabilityTrack').setLevel(logging.DEBUG)

    logging.basicConfig()
    luigi.run()
