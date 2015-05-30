from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap
import os
import luigi
import pybedtools
import shutil
from chipalign.core.task import Task
from chipalign.core.util import temporary_file


class ConsolidatedReads(Task):
    """
    Performs consolidation of reads as described in ROADMAP pipeline.

    Needs AlignedSRR-like Tasks as `input_alignments`.

    Roughly the process is:

    1. Reads from each of the `input_alignments` are lumped together into one big document
    2. If the length of reads is greater than the specified `max_sequencing_depth`,
        the reads are sub-sampled to that size
    3. Reads are then sorted

    The `subsample_random_seed` parameter controls the sub0-sampling
    """
    input_alignments = luigi.Parameter(is_list=True)

    max_sequencing_depth = luigi.IntParameter(default=45000000)
    subsample_random_seed = luigi.IntParameter(default=0)

    def requires(self):
        return self.input_alignments

    @property
    def _extension(self):
        return 'tagAlign.gz'

    def run(self):
        logger = self.logger()

        with temporary_file() as tf:
            with open(tf, 'w') as buffer_:
                for filtered_reads in self.input():
                    logger.debug('Processing {}'.format(filtered_reads.path))
                    filtered_reads_bedtool = pybedtools.BedTool(filtered_reads.path).bam_to_bed()
                    buffer_.writelines(imap(str, filtered_reads_bedtool))
                    logger.debug('.. Done')

            logger.debug('Creating bedtool')
            master_reads_bedtool = pybedtools.BedTool(tf)
            length_of_master_reads = master_reads_bedtool.count()
            logger.debug('Total {} reads'.format(length_of_master_reads))

            if length_of_master_reads > self.max_sequencing_depth:
                logger.debug('Subsampling')

                master_reads_bedtool = master_reads_bedtool.sample(n=self.max_sequencing_depth,
                                                                   seed=self.subsample_random_seed)

            logger.debug('Sorting')
            master_reads_bedtool = master_reads_bedtool.sort()

            logger.debug('Writing to file')
            with temporary_file() as answer:
                master_reads_bedtool.saveas(answer)
                self.ensure_output_directory_exists()
                shutil.move(answer, self.output().path)

        logger.debug('Done')
