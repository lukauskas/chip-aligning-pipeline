from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil

from chipalign.alignment.implementations.base import AlignedReadsBase
from chipalign.sequence.srr import SRRSequence
from chipalign.genome.sequence import GenomeSequence
from chipalign.alignment.implementations.bowtie.index import GenomeIndex


class AlignedReadsPash(AlignedReadsBase):

    genome_version = GenomeIndex.genome_version
    srr_identifier = SRRSequence.srr_identifier

    @property
    def aligner_parameters(self):
        return []

    @property
    def index_task(self):
        return GenomeSequence(genome_version=self.genome_version)

    def run(self):
        from chipalign.command_line_applications.ucsc_suite import twoBitToFa
        from chipalign.command_line_applications.pash import pash3
        from chipalign.command_line_applications.samtools import samtools

        logger = self.logger()

        bam_output_abspath, stdout_output_abspath = self._output_abspaths()

        index_abspath = os.path.abspath(self.index_task.output().path)
        fastq_abspath = os.path.abspath(self.fastq_task._filename().path)

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