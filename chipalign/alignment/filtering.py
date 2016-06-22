from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap, ifilter
import luigi
from chipalign.alignment.aligned_reads import AlignedReads
from chipalign.core.task import Task
from chipalign.core.util import fast_bedtool_from_iterable, autocleaning_pybedtools, temporary_file, \
    timed_segment
from chipalign.genome.chromosomes import Chromosomes
from chipalign.genome.mappability import GenomeMappabilityTrack
import pandas as pd

def _remove_duplicate_reads(bedtools_df):

    ans = bedtools_df.copy()
    ans['pos'] = ans.apply(lambda x: x.end if x.strand == '-' else x.start,
                           axis=1)

    ans = ans.drop_duplicates(subset=['strand', 'chrom', 'pos'], keep=False)
    del ans['pos']

    return ans


def _resize_reads(bedtools_df, new_length,
                  chromsizes):

    new_data = bedtools_df.copy()
    positive_strand = new_data.strand == '+'
    negative_strand = new_data.strand == '-'
    assert (positive_strand | negative_strand).all()

    new_data.loc[positive_strand, 'end'] = new_data.loc[positive_strand, 'start'] + new_length
    new_data.loc[negative_strand, 'start'] = new_data.loc[negative_strand, 'end'] - new_length

    for chrom, (low, high) in chromsizes.items():
        lookup = new_data['chrom'] == chrom
        new_data.loc[lookup, ['start', 'end']] = new_data.loc[lookup, ['start', 'end']].clip(low, high)

    return new_data


class FilteredReads(Task):
    """
    Performs read filtering as described in ROADMAP pipeline.

    1. Truncate (or extend, if needed) the reads to ``resized_length``.
    2. Remove duplicated reads
    3. Filter out all reads that could not be uniquely mapped at ``resized_length``

    The output is always sorted.

    Parameters:
    :param alignment_task: task containing .bam_output() pointing to a BAM file object with aligned reads.
    :param genome_version: self-explanatory; try `hg19`.
    :param resized_length: The length to resize reads to. (default: 36, as set by ROADMAP consortium)
    :param ignore_non_standard_chromosomes: if set to true, non-standard chromosomes will be ignored
    """

    # Task that will be aligned
    alignment_task = luigi.Parameter()

    genome_version = AlignedReads.genome_version
    resized_length = luigi.IntParameter(default=36)  # Roadmap epigenome uses 36

    ignore_non_standard_chromosomes = luigi.BoolParameter(default=True)

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
        reqs = [self.alignment_task, self._mappability_task]
        if self.ignore_non_standard_chromosomes:
            reqs.append(self.standard_chromosomes_task)
        return reqs

    @property
    def _extension(self):
        return 'tagAlign.gz'

    @property
    def standard_chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version,
                           collection='male')  # Male collection contains all of them

    @property
    def task_class_friendly_name(self):
        return 'FR'

    def run(self):
        logger = self.logger()

        bam_output = self.alignment_task.bam_output().path

        with autocleaning_pybedtools() as pybedtools:
            logger.info('Loading {}'.format(bam_output))
            mapped_reads = pybedtools.BedTool(bam_output)
            logger.info('Converting BAM to BED')
            mapped_reads = mapped_reads.bam_to_bed()

            mapped_reads_df = mapped_reads.to_dataframe()
            del mapped_reads  # so we don't accidentally use it

            if self.ignore_non_standard_chromosomes:
                standard_chromosomes = frozenset(
                    self.standard_chromosomes_task.output().load().keys())
                with timed_segment('Leaving only reads within standard chromosomes', logger=logger):
                    _len_before = len(mapped_reads_df)
                    mapped_reads_df = mapped_reads_df[mapped_reads_df.chrom.isin(standard_chromosomes)]
                    _len_after = len(mapped_reads_df)
                    logger.debug('Removed {:,} reads because they are not within standard nucleosomes'.format(_len_after - _len_before))

            if self.resized_length > 0:

                with timed_segment('Truncating reads to be {}bp'.format(self.resized_length),
                                   logger=logger):

                    mapped_reads_df = _resize_reads(mapped_reads_df,
                                                    new_length=self.resized_length,
                                                    chromsizes=pybedtools.chromsizes(
                                                        self.genome_version),
                                                    )

            with timed_segment('Removing duplicates', logger=logger):
                _len_before = len(mapped_reads_df)
                mapped_reads_df = _remove_duplicate_reads(mapped_reads_df)
                _len_after = len(mapped_reads_df)
                logger.debug(
                    'Removed {:,} reads because they were duplicates'.format(
                        _len_after - _len_before))

            with timed_segment('Filtering uniquely mappable', logger=logger):
                mapped_reads_df = self._mappability_task.output().load().filter_uniquely_mappables(
                    mapped_reads_df)

            with timed_segment('Sorting reads', logger=logger):
                mapped_reads_df = mapped_reads_df.sort_values(by=['chrom', 'start', 'end'])

            with timed_segment('Writing to file', logger=logger):
                with self.output().open('w') as f:
                    mapped_reads_df = mapped_reads_df[['chrom', 'start', 'end', 'name', 'score', 'strand']]
                    mapped_reads_df['name'] = "N"  # The alignments from ROADMAP have this
                    mapped_reads_df['score'] = 1000  # And this... for some reason

                    mapped_reads_df.to_csv(f, sep=str('\t'), header=False, index=False)