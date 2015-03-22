from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from multiprocessing import cpu_count
import luigi
import pybedtools
from fastq_sequence import FastqSequence
from genome_browser import GenomeSequence
from genome_mappability import GenomeMappabilityTrack
from task import Task, MetaTask
from ena_downloader import ShortReadsForExperiment
from genome_index import GenomeIndex
import logging
import os
import tempfile
import shutil
from util import ensure_directory_exists_for_file


class BowtieAlignmentTask(Task):

    genome_version = GenomeIndex.genome_version
    experiment_accession = ShortReadsForExperiment.experiment_accession
    study_accession = ShortReadsForExperiment.study_accession

    cell_type = ShortReadsForExperiment.cell_type
    data_track = ShortReadsForExperiment.data_track

    bowtie_seed = luigi.IntParameter(default=0)

    pretrim_reads = luigi.BooleanParameter(default=False)

    number_of_processes = luigi.IntParameter(default=int(cpu_count()/8), significant=False)

    @property
    def short_reads_task(self):
        return ShortReadsForExperiment(experiment_accession=self.experiment_accession,
                                        study_accession=self.study_accession,
                                        cell_type=self.cell_type,
                                        data_track=self.data_track)

    def requires(self):
        return [GenomeIndex(genome_version=self.genome_version),
                self.short_reads_task]


    @property
    def parameters(self):
        params = self.short_reads_task.parameters
        params.append(self.genome_version)

        if self.bowtie_seed != 0:
            params.append(self.bowtie_seed)
        if self.pretrim_reads:
            params.append('trim')

        return params


    @property
    def _extension(self):
        return 'bam'

    def output(self):
        bam_output = super(BowtieAlignmentTask, self).output()

        stdout_output = luigi.File(bam_output.path + '.stdout')

        return bam_output, stdout_output

    def run(self):
        logger = self.logger()

        from command_line_applications.archiving import unzip, tar
        from command_line_applications.bowtie import bowtie2
        from command_line_applications.samtools import samtools

        bam_output, stdout_output = self.output()
        stdout_output_abspath = os.path.abspath(stdout_output.path)
        bam_output_abspath = os.path.abspath(bam_output.path)

        index_output, sra_output = self.input()
        index_abspath = os.path.abspath(index_output.path)
        sra_output_abspath = os.path.abspath(sra_output.path)

        logger.debug('Ensuring output directory exists')
        # Make sure to create directories
        try:
            os.makedirs(os.path.dirname(stdout_output_abspath))
        except OSError:
            if not os.path.isdir(os.path.dirname(stdout_output_abspath)):
                raise

        try:
            os.makedirs(os.path.dirname(bam_output_abspath))
        except OSError:
            if not os.path.isdir(os.path.dirname(bam_output_abspath)):
                raise

        sam_output_filename = 'alignments.sam'
        bam_output_filename = 'alignments.bam'
        stdout_filename = 'stats.txt'

        current_working_directory = os.getcwdu()
        temporary_directory = tempfile.mkdtemp(prefix='tmp-bt2align-')

        try:
            logger.debug('Changing directory to {}'.format(temporary_directory))
            os.chdir(temporary_directory)

            logger.debug('Unzipping index')
            unzip(index_abspath)

            logger.debug('Extracting sequences')
            sequences_dirname = 'sequences'
            os.makedirs(sequences_dirname)
            tar('-xjf', sra_output_abspath, '--directory={}'.format(sequences_dirname))

            sequences_to_align = os.listdir(sequences_dirname)

            if self.pretrim_reads:

                logger.debug('Pretrim sequences set, trimming')
                from command_line_applications.sickle import sickle

                trimmed_sequences_dirname = 'trimmed_sequences'
                os.makedirs(trimmed_sequences_dirname)

                trimmed_sequences = []
                for sequence in sequences_to_align:
                    assert sequence[-3:] == '.gz'
                    sequence_without_gzip = sequence[:-3]

                    logger.debug('Trimming {}'.format(sequence))
                    sickle('se',
                           '-f', os.path.join(sequences_dirname, sequence),
                           '-t', 'sanger',
                           '-o', os.path.join(trimmed_sequences_dirname, sequence_without_gzip))

                    trimmed_sequences.append(sequence_without_gzip)

                logger.debug('Deleting untrimmed sequences')
                shutil.rmtree(sequences_dirname)

                sequences_dirname = trimmed_sequences_dirname
                sequences_to_align = trimmed_sequences

            bowtie_align_string = ','.join([os.path.join(sequences_dirname, x) for x in sequences_to_align])
            logging.debug('Bowtie string to align sequences: {!r}'.format(bowtie_align_string))

            logging.debug('Running bowtie')
            bowtie2('-q', '--very-sensitive',
                    '-U', bowtie_align_string,
                    '-x', self.genome_version,
                    '--no-unal',
                    '-p', self.number_of_processes,
                    '--seed', self.bowtie_seed,
                    '-S', sam_output_filename,
                    _err=stdout_filename
                    )

            logging.debug('Converting SAM to BAM')
            samtools('view', '-b', sam_output_filename, _out=bam_output_filename)

            logging.debug('Moving files to correct locations')
            shutil.move(stdout_filename, stdout_output_abspath)
            shutil.move(bam_output_filename, bam_output_abspath)
            logging.debug('Done')
        finally:
            logger.debug('Cleaning up')
            os.chdir(current_working_directory)
            shutil.rmtree(temporary_directory)

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

        from command_line_applications.archiving import unzip, tar
        from command_line_applications.bowtie import bowtie2
        from command_line_applications.samtools import samtools

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
        from command_line_applications.ucsc_suite import twoBitToFa
        from command_line_applications.pash import pash3
        from command_line_applications.samtools import samtools

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

    truncated_length = luigi.IntParameter(default=36)  # Roadmap epigenome uses 36
    filter_uniquely_mappable_for_truncated_length = luigi.BooleanParameter(default=True)
    remove_duplicates = luigi.BooleanParameter(default=True)  # Also true for roadmap epigenome
    sort = luigi.BooleanParameter(default=True)  # Sort the reads?

    @property
    def _alignment_task(self):
        return AlignedReads(genome_version=self.genome_version,
                            srr_identifier=self.srr_identifier,
                            aligner=self.aligner)

    @property
    def _mappability_task(self):
        if self.truncated_length > 0:
            return GenomeMappabilityTrack(genome_version=self.genome_version,
                                          read_length=self.truncated_length)
        else:
            if self.filter_uniquely_mappable_for_truncated_length:
                raise Exception('Filtering uniquely mappable makes sense only when truncation is used')
            return None

    def requires(self):
        reqs = [self._alignment_task]
        if self._mappability_task is not None:
            reqs.append(self._mappability_task)
        return reqs

    @property
    def _filtering_parameters(self):
        return ['unique' if self.remove_duplicates else 'non-unique',
                't{}'.format(self.truncated_length) if self.truncated_length > 0 else 'untruncated',
                'filtered' if self.filter_uniquely_mappable_for_truncated_length else 'unfiltered',
                'sorted' if self.sort else 'unsorted'
                ]

    @property
    def parameters(self):
        alignment_parameters = self._alignment_task.parameters
        filtering_parameters = self._filtering_parameters
        return alignment_parameters + filtering_parameters

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def run(self):
        logger = self.logger()

        bam_output = self._alignment_task.bam_output().path

        try:
            logger.debug('Loading {}'.format(bam_output))
            mapped_reads = pybedtools.BedTool(bam_output)
            logger.debug('Converting BAM to BED')
            mapped_reads = mapped_reads.bam_to_bed()

            mapped_reads = pybedtools.BedTool(filter(lambda x: x.chrom == 'chr20', mapped_reads))

            if self.truncated_length > 0:
                logger.debug('Truncating reads to {} base pairs'.format(self.truncated_length))
                mapped_reads = _resize_reads(mapped_reads, new_length=self.truncated_length)

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
