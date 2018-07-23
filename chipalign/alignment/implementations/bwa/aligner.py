from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil

import luigi

from chipalign.alignment.implementations.base import AlignedReadsBase
from chipalign.alignment.implementations.bwa.index import BwaIndex
from chipalign.core.util import timed_segment


class AlignedReadsBwa(AlignedReadsBase):
    """
    Returns a set of aligned reads using `BWA`_ aligner.

    see :class:`~chipalign.alignment.implementations.base.AlignedReadsBase` for list of
    supported base parameters.

    Additionally, the class takes two other parameters:

    :param number_of_processes: number of threads to use using alignment. Defaults to 1
    :param seed: the alignment seed to use. Defaults to 0 (i.e. the integer zero, not 'no seed').

    .. _BWA: http://bio-bwa.sourceforge.net/bwa.shtml
    """

    number_of_processes = luigi.IntParameter(default=8, significant=False)
    n_cpu = number_of_processes

    @property
    def aligner_parameters(self):
        return []

    @property
    def index_task(self):
        return BwaIndex(genome_version=self.genome_version)

    def _run(self):

        logger = self.logger()

        from chipalign.command_line_applications.archiving import unzip

        from chipalign.command_line_applications.bwa import bwa
        from chipalign.command_line_applications.samtools import samtools

        bam_output_abspath, stdout_output_abspath = self._output_abspaths()

        index_output_abspath = os.path.abspath(self.index_task.output().path)
        fastq_sequence_abspath = os.path.abspath(self.fastq_task.output().path)

        with self.temporary_directory():

            logger.info('Unpacking index')
            unzip(index_output_abspath)

            sorted_bam_output_filename = 'alignments.sorted.bam'
            stdout_filename = 'stats.txt'

            index_prefix = 'genome.fa'

            with timed_segment('Running BWA (aln + samse) and samtools SAM->BAM', logger=logger):
                # BWA parameters come from encode pipeline
                # https://github.com/ENCODE-DCC/chip-seq-pipeline2/blob/master/src/encode_bwa.py#L73
                samtools(samtools(bwa(bwa('aln', '-q', 5, '-l', 32, '-k', 2,
                                          '-t', self.number_of_processes,
                                          index_prefix,
                                          fastq_sequence_abspath,
                                          _piped=True),
                                      'samse', index_prefix, '-', fastq_sequence_abspath,
                                      _piped=True),
                                  'view', '-b', '-', _piped=True),
                         'sort', '-', '-o', sorted_bam_output_filename,
                         '--threads', self.number_of_processes)

            shutil.move(stdout_filename, stdout_output_abspath)
            shutil.move(sorted_bam_output_filename, bam_output_abspath)
            logger.info('Done')
