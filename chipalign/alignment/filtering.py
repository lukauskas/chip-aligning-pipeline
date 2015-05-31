from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap, ifilter
import luigi
import pybedtools
from chipalign.alignment.aligned_reads import AlignedSRR
from chipalign.core.task import Task
from chipalign.core.util import clean_bedtool_history, fast_bedtool_from_iterable
from chipalign.genome.chromosomes import Chromosomes
from chipalign.genome.mappability import GenomeMappabilityTrack


def _remove_duplicates_from_bed(bedtools_object):

    def _key_for_row(row):
        if row.strand == '+':
            return row.strand, row.chrom, row.start
        elif row.strand == '-':
            return row.strand, row.chrom, row.end
        else:
            raise Exception('No strand information for {!r}'.format(bed_row))

    seen_once = set({})
    seen_more_than_once = set({})

    for bed_row in bedtools_object:

        key = _key_for_row(bed_row)

        if key in seen_once:
            seen_more_than_once.add(key)

        seen_once.add(key)

    del seen_once  # Just in case we need to free up some memory

    filtered_data = ifilter(lambda x: _key_for_row(x) not in seen_more_than_once, bedtools_object)
    return fast_bedtool_from_iterable(filtered_data)


def _resize_reads(bedtools_object, new_length,
                  chromsizes,
                  can_extend=True,
                  can_shorten=True):

    def _resizing_function(row):
        min_value, max_value = chromsizes[row.chrom]

        length = row.end - row.start
        difference = new_length - length

        if not can_extend and difference > 0:
            raise Exception('Read {} is already shorter than {}'.format(row, new_length))
        if not can_shorten and difference < 0:
            raise Exception('Read {} is already longer than {}'.format(row, new_length))

        if row.strand == '+':
            row.end = min(row.end + difference, max_value)
        elif row.strand == '-':
            row.start = max(min_value, row.start - difference)
        else:
            raise Exception('Data without strand information provided to _resize_reads')

        return row

    new_data = imap(_resizing_function, bedtools_object)
    return fast_bedtool_from_iterable(new_data)

def _filter_chromosomes(bedtools_object, allowed_chromosomes):
    allowed_chromosomes = frozenset(allowed_chromosomes)

    new_data = ifilter(lambda x: x.chrom in allowed_chromosomes, bedtools_object)
    return fast_bedtool_from_iterable(new_data)

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

    genome_version = AlignedSRR.genome_version
    resized_length = luigi.IntParameter(default=36)  # Roadmap epigenome uses 36

    ignore_non_standard_chromosomes = luigi.BooleanParameter(default=True)

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
    def genome_version(self):
        return self.alignment_task.genome_version

    @property
    def standard_chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version,
                           collection='male')  # Male collection contains all of them

    def run(self):
        logger = self.logger()

        bam_output = self.alignment_task.bam_output().path

        mapped_reads = None
        try:
            logger.info('Loading {}'.format(bam_output))
            mapped_reads = pybedtools.BedTool(bam_output)
            logger.info('Converting BAM to BED')
            mapped_reads = mapped_reads.bam_to_bed()

            if self.ignore_non_standard_chromosomes:
                logger.info('Leaving only reads within standard chromosomes')
                standard_chromosomes = self.standard_chromosomes_task.output().load().keys()
                logger.debug('Standard chromosomes: {}'.format(standard_chromosomes))
                _len_before = mapped_reads.count()
                mapped_reads = _filter_chromosomes(mapped_reads, standard_chromosomes)
                logger.debug('Number of reads removed: {}'.format(mapped_reads.count() - _len_before))

            if self.resized_length > 0:
                logger.info('Truncating reads to {} base pairs'.format(self.resized_length))
                resized_reads = _resize_reads(mapped_reads,
                                              new_length=self.resized_length,
                                              chromsizes=pybedtools.chromsizes(self.genome_version),
                                              can_shorten=True,
                                              # According to the Roadmap protocol
                                              # this should be false
                                              # but since the reads they are pre-processing are 200 bp long
                                              # and we are working with raw reads, some datasets actually have shorter
                                              # ones. Meaning their mappability unification routine is a bit off.
                                              can_extend=True,
                                              )

                mapped_reads.delete_temporary_history(ask=False)
                mapped_reads = resized_reads

            logger.info('Removing duplicates. Length before: {}'.format(mapped_reads.count()))
            mapped_reads = _remove_duplicates_from_bed(mapped_reads)
            logger.info('Done removing duplicates. Length after: {}'.format(mapped_reads.count()))

            logger.info('Filtering uniquely mappable')
            mapped_reads = self._mappability_task.output().load().filter_uniquely_mappables(mapped_reads)

            logger.info('Sorting reads')
            mapped_reads = mapped_reads.sort()

            logger.info('Writing to file')
            with self.output().open('w') as f:

                for row in mapped_reads:
                    row.name = 'N'  # The alignments from ROADMAP have this
                    row.score = '1000'  # And this... for some reason
                    f.write(str(row))
        finally:
            if mapped_reads:
                clean_bedtool_history(mapped_reads)
