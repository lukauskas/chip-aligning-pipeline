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

from chipalign.genome.chromosomes import Chromosomes
from chipalign.core.downloader import fetch
from chipalign.genome.fastq_sequence import FastqSequence
from chipalign.genome.genome_browser import GenomeSequence
from chipalign.genome.genome_mappability import GenomeMappabilityTrack
from chipalign.core.task import Task, MetaTask
from chipalign.genome.genome_index import GenomeIndex
from chipalign.core.util import ensure_directory_exists_for_file


class AlignedReadsBase(Task):
    genome_version = GenomeIndex.genome_version
    srr_identifier = FastqSequence.srr_identifier

    @property
    def fastq_task(self):
        return FastqSequence(srr_identifier=self.srr_identifier)

    @property
    def index_task(self):
        raise NotImplementedError

    def requires(self):
        return [self.fastq_task, self.index_task]

    @property
    def parameters(self):
        fastq_parameters = self.fastq_task.parameters
        genome_index_parameters = self.index_task.parameters

        aligner_parameters = self.aligner_parameters

        return fastq_parameters + genome_index_parameters + aligner_parameters

    @property
    def aligner_parameters(self):
        raise NotImplementedError

    @property
    def _extension(self):
        return 'bam'


    def output(self):
        bam_output = super(AlignedReadsBase, self).output()
        stdout_output = luigi.File(bam_output.path + '.stdout')
        return bam_output, stdout_output

    def _output_abspaths(self, ensure_directory_exists=True):

        bam_output, stdout_output = self.output()
        stdout_output_abspath = os.path.abspath(stdout_output.path)
        bam_output_abspath = os.path.abspath(bam_output.path)

        if ensure_directory_exists:
            ensure_directory_exists_for_file(stdout_output_abspath)
            ensure_directory_exists_for_file(bam_output_abspath)

        return bam_output_abspath, stdout_output_abspath

class AlignedReadsBowtie(AlignedReadsBase):

    number_of_processes = luigi.IntParameter(default=1, significant=False)
    seed = luigi.IntParameter(default=0)

    @property
    def aligner_parameters(self):
        bowtie_parameters = ['s{}'.format(self.seed)]
        return bowtie_parameters

    @property
    def index_task(self):
        return GenomeIndex(genome_version=self.genome_version)

    def run(self):

        logger = self.logger()

        from chipalign.command_line_applications.archiving import unzip
        from chipalign.command_line_applications.bowtie import bowtie2
        from chipalign.command_line_applications import samtools

        bam_output_abspath, stdout_output_abspath = self._output_abspaths()

        index_output_abspath = os.path.abspath(self.index_task.output().path)
        fastq_sequence_abspath = os.path.abspath(self.fastq_task.output().path)

        with self.temporary_directory():
            logger.debug('Unzipping index')
            unzip(index_output_abspath)

            sam_output_filename = 'alignments.sam'
            bam_output_filename = 'alignments.bam'
            stdout_filename = 'stats.txt'

            logger.debug('Running bowtie')
            bowtie2('-U', fastq_sequence_abspath,
                    '-x', self.genome_version,
                    '-p', self.number_of_processes,
                    '--seed', self.seed,
                    '-S', sam_output_filename,
                    '--mm',
                    _err=stdout_filename
                    )

            logger.debug('Converting SAM to BAM')
            samtools('view', '-b', sam_output_filename, _out=bam_output_filename)

            logger.debug('Moving files to correct locations')
            shutil.move(stdout_filename, stdout_output_abspath)
            shutil.move(bam_output_filename, bam_output_abspath)
            logger.debug('Done')

class AlignedReadsPash(AlignedReadsBase):

    genome_version = GenomeIndex.genome_version
    srr_identifier = FastqSequence.srr_identifier

    @property
    def aligner_parameters(self):
        return []

    @property
    def index_task(self):
        return GenomeSequence(genome_version=self.genome_version)

    def run(self):
        from chipalign.command_line_applications.ucsc_suite import twoBitToFa
        from chipalign.command_line_applications.pash import pash3
        from chipalign.command_line_applications import samtools

        logger = self.logger()

        bam_output_abspath, stdout_output_abspath = self._output_abspaths()

        index_abspath = os.path.abspath(self.index_task.output().path)
        fastq_abspath = os.path.abspath(self.fastq_task.output().path)

        with self.temporary_directory():

            logger.debug('Dumping 2bit sequence to fa format')
            index_fa = 'index.fa'
            twoBitToFa(index_abspath, index_fa)

            output_file = 'alignments.sam'

            stdout_filename = 'stdout'

            logger.debug('Running Pash')
            pash3('-N', 1,                 # maximum one match per read
                  '-g', index_fa,          # reference genome
                  '-r', fastq_abspath,
                  '-o', output_file,
                  _err=stdout_filename)

            logger.debug('Converting SAM to BAM')
            output_file_bam = 'alignments.bam'
            samtools('view', '-b', output_file, _out=output_file_bam)

            logger.debug('Moving files to correct locations')
            shutil.move(stdout_filename, stdout_output_abspath)
            shutil.move(output_file_bam, bam_output_abspath)
            logger.debug('Done')

class AlignedReads(MetaTask):

    genome_version = GenomeIndex.genome_version
    srr_identifier = FastqSequence.srr_identifier

    aligner = luigi.Parameter()

    def requires(self):
        if self.aligner == 'bowtie':
            class_ = AlignedReadsBowtie
        elif self.aligner == 'pash':
            class_ = AlignedReadsPash
        else:
            raise Exception('Aligner {} is not supported'.format(self.aligner))

        return class_(genome_version=self.genome_version, srr_identifier=self.srr_identifier)

    @property
    def self_parameters(self):
        return [self.aligner]

    def bam_output(self):
        return self.output()[0]

def _remove_duplicates_from_bed(bedtools_object):

    def _key_for_row(row):
        if row.strand == '+':
            return row.strand, row.chrom, row.start
        elif row.strand == '-':
            return row.strand, row.chrom, row.end
        else:
            raise Exception('No strand information for {!r}'.format(bed_row))

    seen_once = set({})
    seen_more_than_once = set({})

    for bed_row in bedtools_object:

        key = _key_for_row(bed_row)

        if key in seen_once:
            seen_more_than_once.add(key)

        seen_once.add(key)

    del seen_once  # Just in case we need to free up some memory

    filtered_data = filter(lambda x: _key_for_row(x) not in seen_more_than_once, bedtools_object)
    return pybedtools.BedTool(filtered_data)

def _resize_reads(bedtools_object, new_length, chromsizes,
                  can_extend=True,
                  can_shorten=True):

    def _resizing_function(row):
        min_value, max_value = chromsizes[row.chrom]

        length = row.end - row.start
        difference = new_length - length

        if not can_extend and difference > 0:
            raise Exception('Read {} is already shorter than {}'.format(row, new_length))
        if not can_shorten and difference < 0:
            raise Exception('Read {} is already longer than {}'.format(row, new_length))

        if row.strand == '+':
            row.end = min(row.end + difference, max_value)
        elif row.strand == '-':
            row.start = max(min_value, row.start - difference)
        else:
            raise Exception('Data without strand information provided to _truncate_reads')

        return row

    new_data = map(_resizing_function, bedtools_object)
    return pybedtools.BedTool(new_data)

def _filter_uniquely_mappable(mappability_track, reads):
    return pybedtools.BedTool(filter(lambda x: mappability_track.is_uniquely_mappable(x.chrom, x.start, x.end, x.strand), reads))

class FilteredReads(Task):

    genome_version = AlignedReads.genome_version
    srr_identifier = AlignedReads.srr_identifier
    aligner = AlignedReads.aligner

    resized_length = luigi.IntParameter(default=36)  # Roadmap epigenome uses 36
    filter_uniquely_mappable_for_truncated_length = luigi.BooleanParameter(default=True)
    remove_duplicates = luigi.BooleanParameter(default=True)  # Also true for roadmap epigenome
    sort = luigi.BooleanParameter(default=True)  # Sort the reads?

    chromosomes = Chromosomes.collection

    @property
    def _alignment_task(self):
        return AlignedReads(genome_version=self.genome_version,
                            srr_identifier=self.srr_identifier,
                            aligner=self.aligner)

    @property
    def _chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version,
                           collection=self.chromosomes)

    @property
    def _mappability_task(self):
        if self.resized_length > 0:
            return GenomeMappabilityTrack(genome_version=self.genome_version,
                                          read_length=self.resized_length)
        else:
            if self.filter_uniquely_mappable_for_truncated_length:
                raise Exception('Filtering uniquely mappable makes sense only when truncation is used')
            return None

    def requires(self):
        reqs = [self._alignment_task, self._chromosomes_task]
        if self._mappability_task is not None:
            reqs.append(self._mappability_task)
        return reqs

    @property
    def filtering_parameters(self):
        return ['unique' if self.remove_duplicates else 'non-unique',
                't{}'.format(self.resized_length) if self.resized_length > 0 else 'untruncated',
                'filtered' if self.filter_uniquely_mappable_for_truncated_length else 'unfiltered',
                'sorted' if self.sort else 'unsorted'
                ]

    @property
    def parameters(self):
        alignment_parameters = self._alignment_task.parameters
        filtering_parameters = self.filtering_parameters
        self_parameters = [self.chromosomes]
        return alignment_parameters + filtering_parameters + self_parameters

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def run(self):
        logger = self.logger()

        bam_output = self._alignment_task.bam_output().path

        chromsizes = self._chromosomes_task.output().load()

        try:
            logger.debug('Loading {}'.format(bam_output))
            mapped_reads = pybedtools.BedTool(bam_output)
            logger.debug('Converting BAM to BED')
            mapped_reads = mapped_reads.bam_to_bed()

            logger.debug('Leaving only chromosomes in chromsizes')
            mapped_reads = pybedtools.BedTool(filter(lambda x: x.chrom in chromsizes, mapped_reads))

            if self.resized_length > 0:
                logger.debug('Truncating reads to {} base pairs'.format(self.resized_length))
                mapped_reads = _resize_reads(mapped_reads,
                                             new_length=self.resized_length,
                                             chromsizes=chromsizes,
                                             can_shorten=True,
                                             # According to the Roadmap protocol
                                             # this should be false
                                             # but since the reads they are pre-processing are 200 bp long
                                             # and we are working with raw reads, some datasets actually have shorter
                                             # ones. Meaning their mappability unification routine is a bit off.
                                             can_extend=True,
                                             )

            if self.remove_duplicates:
                logger.debug('Removing duplicates. Length before: {}'.format(len(mapped_reads)))
                mapped_reads = _remove_duplicates_from_bed(mapped_reads)
                logger.debug('Done removing duplicates. Length after: {}'.format(len(mapped_reads)))

            if self.filter_uniquely_mappable_for_truncated_length:
                logger.debug('Filtering uniquely mappable')
                mapped_reads = self._mappability_task.output().load().filter_uniquely_mappables(mapped_reads)

            if self.sort:
                logger.debug('Sorting reads')
                mapped_reads = mapped_reads.sort()

            logger.debug('Writing to file')
            with self.output().open('w') as f:

                for row in mapped_reads:
                    row.name = 'N'  # The alignments from roadmap have this
                    row.score = '1000'  # And this... for some reason
                    f.write(str(row))

        finally:
            pybedtools.cleanup()

class ConsolidatedReads(Task):

    srr_identifiers = luigi.Parameter(is_list=True)

    genome_version = FilteredReads.genome_version
    aligner = FilteredReads.aligner

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
                      aligner=self.aligner,
                      resized_length=self.resized_length,
                      filter_uniquely_mappable_for_truncated_length=self.filter_uniquely_mappable_for_truncated_length,
                      remove_duplicates=self.remove_duplicates,
                      sort=self.sort,
                      chromosomes=self.chromosomes)

        tasks = []
        for srr_identifier in self.srr_identifiers:
            tasks.append(FilteredReads(srr_identifier=srr_identifier, **kwargs))

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
