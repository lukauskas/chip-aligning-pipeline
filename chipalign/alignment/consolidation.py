from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap, ifilter
import os
import luigi
import shutil
from chipalign.core.task import Task
from chipalign.core.util import temporary_file, autocleaning_pybedtools
from chipalign.genome.chromosomes import Chromosomes


class ConsolidatedReads(Task):
    """
    Performs consolidation of reads as described in ROADMAP pipeline.

    Needs AlignedSRR-like Tasks as `input_alignments`.

    Roughly the process is:

    1. Reads from each of the `input_alignments` are lumped together into one big document
    2. Reads to non-standard chromosomes (i.e. 'chr6_dbb_hap3') are removed (if `use_only_standard_chromosomes`)
    3. If the length of reads is greater than the specified `max_sequencing_depth`,
        the reads are sub-sampled to that size
    4. Reads are then sorted

    The `subsample_random_seed` parameter controls the sub0-sampling.

    :param input_alignments: Input alignments to use.
    :type input_alignments: an iterable of :class:`~chipalign.alignment.aligned_reads.AlignedSRR`
                            tasks
    :param max_sequencing_depth: Max sequencing depth to use, defaults to 45,000,000.
                                 Consult the `metadata spreadsheet`_ for appropriate values
    :param subsample_random_seed: random seed to use for subsampled data. Defaults to integer 0.
    :param use_only_standard_chromosomes: if set to true, only standard chromosomes will be used
                                          in subsampling Non standard chromosomes such as
                                          'chr6_dbb_hap3' will be removed. Defaults to True.

    .. _metadata spreadsheet: https://docs.google.com/spreadsheet/ccc?key=0Am6FxqAtrFDwdHU1UC13ZUxKYy1XVEJPUzV6MEtQOXc&usp=sharing
    """
    input_alignments = luigi.Parameter()

    max_sequencing_depth = luigi.IntParameter(default=45000000)
    subsample_random_seed = luigi.IntParameter(default=0)

    use_only_standard_chromosomes = luigi.BoolParameter(default=True)

    _parameter_names_to_hash = ('input_alignments', )

    @property
    def genome_version(self):
        genome_version = None
        for input_task in self.input_alignments:
            if genome_version is None:
                genome_version = input_task.genome_version
            elif genome_version != input_task.genome_version:
                raise Exception('Inconsistent genome versions for input')

        return genome_version

    @property
    def standard_chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version,
                           collection='male')  # Male collection contains all of them

    def requires(self):
        reqs = list(self.input_alignments)
        __ = self.genome_version   # Establish that all inputs are of teh same genome version
        if self.use_only_standard_chromosomes:
            reqs.append(self.standard_chromosomes_task)
        return reqs

    @property
    def _extension(self):
        return 'tagAlign.gz'

    @property
    def task_class_friendly_name(self):
        return 'CR'

    def run(self):
        logger = self.logger()

        if self.use_only_standard_chromosomes:
            chromosomes = self.standard_chromosomes_task.output().load()
            chromosomes = frozenset(chromosomes.keys())

            chromosome_filter = lambda x: x.chrom in chromosomes
        else:
            chromosome_filter = lambda x: True

        with autocleaning_pybedtools() as pybedtools:
            with temporary_file() as tf:
                with open(tf, 'w') as tf_file_handle:
                    for filtered_reads in self.input_alignments:
                        filtered_reads = filtered_reads.output()
                        logger.debug('Processing {}'.format(filtered_reads.path))
                        filtered_reads_bedtool = pybedtools.BedTool(filtered_reads.path)

                        tf_file_handle.writelines(imap(str,
                                                  ifilter(chromosome_filter,
                                                          filtered_reads_bedtool)))
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

                    # Write the output in correct format (gzipped)
                    with self.output().open('w') as out_:
                        with open(answer) as in_:
                            out_.writelines(in_)

        logger.debug('Done')
