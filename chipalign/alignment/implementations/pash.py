from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil

from chipalign.alignment.implementations.base import AlignedReadsBase
from chipalign.genome.sequence import GenomeSequence


class AlignedReadsPash(AlignedReadsBase):
    """
    Returns a set of aligned reads using `Pash 3.0`_ aligner.

    see :class:`~chipalign.alignment.implementations.base.AlignedReadsBase` for list of
    supported parameters.

    .. _Pash 3.0: http://bmcbioinformatics.biomedcentral.com/articles/10.1186/1471-2105-11-572
    """

    @property
    def aligner_parameters(self):
        return []

    @property
    def index_task(self):
        return GenomeSequence(genome_version=self.genome_version)

    def _run(self):
        from chipalign.command_line_applications.ucsc_suite import twoBitToFa
        from chipalign.command_line_applications.pash import pash3
        from chipalign.command_line_applications.samtools import samtools

        logger = self.logger()

        bam_output_abspath, stdout_output_abspath = self._output_abspaths()

        index_abspath = os.path.abspath(self.index_task.output().path)
        fastq_abspath = os.path.abspath(self.fastq_task.output().path)

        with self.temporary_directory():

            logger.info('Dumping 2bit sequence to fa format')
            index_fa = 'index.fa'
            twoBitToFa(index_abspath, index_fa)

            output_file = 'alignments.sam'

            stdout_filename = 'stdout'

            logger.info('Running Pash')
            pash3('-N', 1,                 # maximum one match per read
                  '-g', index_fa,          # reference genome
                  '-r', fastq_abspath,
                  '-o', output_file,
                  _err=stdout_filename)

            logger.info('Converting SAM to BAM')
            output_file_bam = 'alignments.bam'
            samtools('view', '-b', output_file, _out=output_file_bam)

            logger.info('Moving files to correct locations')
            shutil.move(stdout_filename, stdout_output_abspath)
            shutil.move(output_file_bam, bam_output_abspath)
            logger.info('Done')
