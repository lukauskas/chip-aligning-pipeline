from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import shutil

import luigi
from chipalign.alignment.aligned_reads import AlignedReads
from chipalign.core.task import Task
from chipalign.core.util import fast_bedtool_from_iterable, autocleaning_pybedtools, temporary_file, \
    timed_segment
from chipalign.genome.chromosomes import Chromosomes
from chipalign.genome.mappability import GenomeMappabilityTrack


def _remove_duplicate_reads_inplace(bedtools_df):
    bedtools_df['pos'] = bedtools_df['end'].where(bedtools_df['strand'] == '-',
                                                  bedtools_df['start'])
    bedtools_df.drop_duplicates(subset=['strand', 'chrom', 'pos'], keep=False, inplace=True)
    del bedtools_df['pos']


def _resize_reads_inplace(bedtools_df, new_length,
                          chromsizes):

    positive_strand = bedtools_df.strand == '+'
    negative_strand = bedtools_df.strand == '-'
    assert (positive_strand | negative_strand).all()

    bedtools_df.loc[positive_strand, 'end'] = bedtools_df.loc[positive_strand, 'start'] + new_length
    bedtools_df.loc[negative_strand, 'start'] = bedtools_df.loc[negative_strand, 'end'] - new_length

    for chrom, (low, high) in chromsizes.items():
        lookup = bedtools_df['chrom'] == chrom
        bedtools_df.loc[lookup, ['start', 'end']] = bedtools_df.loc[lookup, ['start', 'end']].clip(low, high)


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
        self.ensure_output_directory_exists()

        bam_output = self.alignment_task.bam_output().path

        with autocleaning_pybedtools() as pybedtools:
            logger.info('Loading {}'.format(bam_output))
            mapped_reads = pybedtools.BedTool(bam_output)
            logger.info('Converting BAM to BED')
            mapped_reads = mapped_reads.bam_to_bed()

            mapped_reads_df = mapped_reads.to_dataframe()
            del mapped_reads  # so we don't accidentally use it

            # Get chromsizes
            chromsizes = pybedtools.chromsizes(self.genome_version)

        # Go out of pybedtools scope so it can clean up

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

                _resize_reads_inplace(mapped_reads_df,
                                      new_length=self.resized_length,
                                      chromsizes=chromsizes)

        with timed_segment('Removing duplicates', logger=logger):
            _len_before = len(mapped_reads_df)
            _remove_duplicate_reads_inplace(mapped_reads_df)
            _len_after = len(mapped_reads_df)
            logger.debug(
                'Removed {:,} reads because they were duplicates'.format(
                    _len_after - _len_before))

        with timed_segment('Filtering uniquely mappable', logger=logger):
            mappability_filter = self._mappability_task.output().load()
            mapped_reads_df = mappability_filter.filter_uniquely_mappables(
                mapped_reads_df)

        with timed_segment('Sorting reads inplace', logger=logger):
            mapped_reads_df.sort_values(by=['chrom', 'start', 'end'], inplace=True)

        with timed_segment('Writing to file', logger=logger):
            with temporary_file() as tf:
                mapped_reads_df['name'] = "N"  # The alignments from ROADMAP have this
                mapped_reads_df['score'] = 1000  # And this... for some reason

                mapped_reads_df.to_csv(tf, sep='\t', header=False, index=False,
                                       compression='gzip',
                                       columns=['chrom', 'start', 'end', 'name',
                                                'score', 'strand'])

                shutil.move(tf, self.output().path)
