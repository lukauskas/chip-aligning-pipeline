from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from multiprocessing import cpu_count
import luigi
from task import Task
from ena_downloader import ShortReadsForExperiment
from genome_index import GenomeIndex
import logging
import os
import tempfile
import shutil

class BowtieAlignmentTask(Task):

    genome_version = GenomeIndex.genome_version
    experiment_accession = ShortReadsForExperiment.experiment_accession
    study_accession = ShortReadsForExperiment.study_accession
    experiment_alias = ShortReadsForExperiment.experiment_alias

    bowtie_seed = luigi.IntParameter(default=0)

    pretrim_reads = luigi.BooleanParameter(default=False)

    number_of_processes = luigi.IntParameter(default=cpu_count()/8, significant=False)

    def requires(self):
        return [GenomeIndex(genome_version=self.genome_version),
                ShortReadsForExperiment(experiment_accession=self.experiment_accession,
                                        study_accession=self.study_accession,
                                        experiment_alias=self.experiment_alias)]


    @property
    def parameters(self):
        params = [self.study_accession,
                  self.experiment_accession,
                  self.experiment_alias,
                  self.genome_version]

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
        logger = logging.getLogger('BowtieAlignmentTask')

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


if __name__ == '__main__':
    logging.getLogger('BowtieAlignmentTask').setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()
