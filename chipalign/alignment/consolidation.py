import luigi
import shutil

from chipalign.core.task import Task
from chipalign.core.util import temporary_file, timed_segment
from chipalign.genome.chromosomes import Chromosomes

import pandas as pd
import numpy as np

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

    max_sequencing_depth = luigi.IntParameter(default=30000000)
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

    def _reads_to_table(self, filename, chromosomes):
        fr = pd.read_table(filename,
                           header=None,
                           names=['chromosome', 'start', 'end', 'name', 'score', 'strand'])

        if chromosomes:
            fr = fr[fr.chromosome.isin(chromosomes)]

        return fr

    def _subsample_mask(self, n_reads, subsample_size, random_state):
        random = np.random.RandomState(random_state)

        indices = random.choice(np.arange(n_reads, dtype=int),
                                subsample_size,
                                replace=False)

        mask = np.zeros(n_reads, dtype=bool)
        mask[indices] = True

        return mask

    def _run(self):
        logger = self.logger()
        self.ensure_output_directory_exists()

        if self.use_only_standard_chromosomes:
            chromosomes = self.standard_chromosomes_task.output().load()
            chromosomes = frozenset(chromosomes.keys())
        else:
            chromosomes = None

        n_reads = 0

        with timed_segment('Computing total number of reads', logger=self.logger()):

            for filtered_reads in self.input_alignments:
                filtered_reads = filtered_reads.output()
                n_reads += len(self._reads_to_table(filtered_reads.path, chromosomes))

        logger.debug('Number of reads parsed: {:,}'.format(n_reads))

        if n_reads <= self.max_sequencing_depth:
            mask = np.ones(n_reads, dtype=bool)
        else:
            mask = self._subsample_mask(n_reads, subsample_size=self.max_sequencing_depth,
                                        random_state=self.subsample_random_seed)

        with timed_segment('Concatenating subsampled reads', logger=self.logger()):
            reads = []
            pos = 0
            for filtered_reads in self.input_alignments:
                filtered_reads = filtered_reads.output()

                fr = self._reads_to_table(filtered_reads.path, chromosomes)
                len_ = len(fr)
                current_mask = mask[pos:pos+len_]
                fr = fr[current_mask]

                reads.append(fr)
                pos += len_

            reads = pd.concat(reads)

        with timed_segment('Sorting concatenated reads', logger=self.logger()):
            reads = reads.sort_values(by=['chromosome', 'start', 'end'])

        logger.info("Writing to file")
        with temporary_file() as tf:
            reads.to_csv(tf, sep='\t',
                         compression='gzip',
                         header=False, index=False)
            shutil.move(tf, self.output().path)
