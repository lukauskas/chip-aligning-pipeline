from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil

import luigi

from chipalign.alignment.implementations.base import AlignedReadsBase
from chipalign.alignment.implementations.bowtie.index import BowtieIndex
from chipalign.core.util import timed_segment


class AlignedReadsBowtie(AlignedReadsBase):
    """
    Returns a set of aligned reads using `Bowtie 2`_ aligner.

    see :class:`~chipalign.alignment.implementations.base.AlignedReadsBase` for list of
    supported base parameters.

    Additionally, the class takes two other parameters:

    :param number_of_processes: number of threads to use using alignment. Defaults to 1
    :param seed: the alignment seed to use. Defaults to 0 (i.e. the integer zero, not 'no seed').

    .. _Bowtie 2: http://bowtie-bio.sourceforge.net/bowtie2/index.shtml
    """

    number_of_processes = luigi.IntParameter(default=1, significant=False)
    seed = luigi.IntParameter(default=0)

    @property
    def aligner_parameters(self):
        bowtie_parameters = ['s{}'.format(self.seed)]
        return bowtie_parameters

    @property
    def index_task(self):
        return BowtieIndex(genome_version=self.genome_version)

    def _run(self):

        logger = self.logger()

        from chipalign.command_line_applications.archiving import unzip
        from chipalign.command_line_applications.bowtie import bowtie2
        from chipalign.command_line_applications.samtools import samtools

        bam_output_abspath, stdout_output_abspath = self._output_abspaths()

        index_output_abspath = os.path.abspath(self.index_task.output().path)
        fastq_sequence_abspath = os.path.abspath(self.fastq_task.output().path)

        with self.temporary_directory():
            logger.info('Unzipping index')
            unzip(index_output_abspath)

            sam_output_filename = 'alignments.sam'
            bam_output_filename = 'alignments.bam'
            stdout_filename = 'stats.txt'

            with timed_segment('Running bowtie', logger=logger):
                bowtie2('-U', fastq_sequence_abspath,
                        '-x', self.genome_version,
                        '-p', self.number_of_processes,
                        '--seed', self.seed,
                        '-S', sam_output_filename,
                        '--mm',
                        _err=stdout_filename
                        )

            logger.info('Converting SAM to BAM')
            samtools('view', '-b', sam_output_filename, _out=bam_output_filename)

            logger.info('Moving files to correct locations')
            shutil.move(stdout_filename, stdout_output_abspath)
            shutil.move(bam_output_filename, bam_output_abspath)
            logger.info('Done')
