from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil

import luigi

from chipalign.alignment.implementations.base import AlignedReadsBase
from chipalign.alignment.implementations.bowtie.index import BowtieIndex


class AlignedReadsBowtie(AlignedReadsBase):

    number_of_processes = luigi.IntParameter(default=1, significant=False)
    seed = luigi.IntParameter(default=0)

    @property
    def aligner_parameters(self):
        bowtie_parameters = ['s{}'.format(self.seed)]
        return bowtie_parameters

    @property
    def index_task(self):
        return BowtieIndex(genome_version=self.genome_version)

    def run(self):

        logger = self.logger()

        from chipalign.command_line_applications.archiving import unzip
        from chipalign.command_line_applications.bowtie import bowtie2
        from chipalign.command_line_applications.samtools import samtools

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