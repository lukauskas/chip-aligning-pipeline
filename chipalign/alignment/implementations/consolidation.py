from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
import pybedtools
from chipalign.core.task import Task


class ConsolidatedReads(Task):
    """
    Performs consolidation of reads as described in ROADMAP pipeline.

    Needs AlignedSRR-like Tasks as `input_alignments`.
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
        master_reads_bedtool = None

        try:
            master_reads = []

            logger = self.logger()

            for filtered_reads in self.input():
                logger.debug('Processing {}'.format(filtered_reads.path))
                master_reads.extend(pybedtools.BedTool(filtered_reads.path))

            master_reads_bedtool = pybedtools.BedTool(master_reads)
            length_of_master_reads = len(master_reads)

            logger.debug('Total {} reads'.format(length_of_master_reads))
            if length_of_master_reads > self.max_sequencing_depth:
                logger.debug('Subsampling')

                master_reads_bedtool = master_reads_bedtool.sample(n=self.max_sequencing_depth,
                                                                   seed=self.subsample_random_seed)

                logger.debug('Sorting')
                master_reads_bedtool = master_reads_bedtool.sort()

            logger.debug('Writing to file')
            with self.output().open('w') as f:
                for row in master_reads:
                    f.write(str(row))
            logger.debug('Done')

        finally:
            if master_reads_bedtool:
                master_reads_bedtool.delete_temporary_history(ask=False)
