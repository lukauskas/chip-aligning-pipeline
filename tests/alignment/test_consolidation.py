from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap, ifilter
import os
from random import Random
import luigi
import pybedtools
import shutil
from chipalign.alignment.implementations.consolidation import ConsolidatedReads
from chipalign.core.task import Task
from chipalign.core.util import temporary_file
from tests.helpers.task_test import TaskTestCase


class TestReadConsolidation(TaskTestCase):
    _GENOME = 'hg19'

    def test_random_reads_no_subsampling_consolidated_correctly_over_limit(self):
        length_of_reads = 100

        reads_a = RandomAlignedReads(seed=1, length_of_reads=length_of_reads, number_of_reads=50,
                                     genome_version=self._GENOME)
        reads_b = RandomAlignedReads(seed=2, length_of_reads=length_of_reads, number_of_reads=150,
                                     genome_version=self._GENOME)

        max_sequencing_depth = 200  # no subsampling should be triggered

        consolidated_reads_task = ConsolidatedReads(input_alignments=[reads_a, reads_b],
                                                    max_sequencing_depth=max_sequencing_depth,
                                                    subsample_random_seed=0,
                                                    use_only_standard_chromosomes=False)
        self.build_task(consolidated_reads_task)

        with temporary_file() as tf:
            with open(tf, 'w') as f:
                f.writelines(imap(str, pybedtools.BedTool(reads_a.output().path).bam_to_bed()))
                f.writelines(imap(str, pybedtools.BedTool(reads_b.output().path).bam_to_bed()))

            joint_input_bedtool = pybedtools.BedTool(tf)
            joint_input_bedtool = joint_input_bedtool.sort()

            answer_bedtool = pybedtools.BedTool(consolidated_reads_task.output().path)
            self.assertListEqual(list(joint_input_bedtool), list(answer_bedtool))

    def test_random_reds_subsampled_correctly(self):
        length_of_reads = 100

        reads_a = RandomAlignedReads(seed=1, length_of_reads=length_of_reads, number_of_reads=50,
                                     genome_version=self._GENOME)
        reads_b = RandomAlignedReads(seed=2, length_of_reads=length_of_reads, number_of_reads=150,
                                     genome_version=self._GENOME)

        max_sequencing_depth = 100  # Should be subsampled nicely

        consolidated_reads_task = ConsolidatedReads(input_alignments=[reads_a, reads_b],
                                                    max_sequencing_depth=max_sequencing_depth,
                                                    subsample_random_seed=0,
                                                    use_only_standard_chromosomes=False)
        self.build_task(consolidated_reads_task)

        answer_bedtool = pybedtools.BedTool(consolidated_reads_task.output().path)
        self.assertEqual(max_sequencing_depth, answer_bedtool.count())
        sorted_ = answer_bedtool.sort()
        self.assertEqual(answer_bedtool, sorted_, 'Bedtool that is returned was unsorted')

    def test_nonstandard_chromosomes_are_removed(self):
        length_of_reads = 100

        reads_a = RandomAlignedReads(seed=1, length_of_reads=length_of_reads,
                                     number_of_reads=50,
                                     number_of_nonstandard_reads=10,
                                     genome_version=self._GENOME)
        reads_b = RandomAlignedReads(seed=2, length_of_reads=length_of_reads,
                                     number_of_reads=150,
                                     number_of_nonstandard_reads=10,
                                     genome_version=self._GENOME)

        max_sequencing_depth = 200  # no subsampling should be triggered

        consolidated_reads_task = ConsolidatedReads(input_alignments=[reads_a, reads_b],
                                                    max_sequencing_depth=max_sequencing_depth,
                                                    subsample_random_seed=0,
                                                    use_only_standard_chromosomes=True)
        self.build_task(consolidated_reads_task)

        standard_chromosomes_filter = lambda x: '_' not in x.chrom

        with temporary_file() as tf:
            with open(tf, 'w') as f:
                f.writelines(imap(str,
                                  ifilter(standard_chromosomes_filter,
                                          pybedtools.BedTool(reads_a.output().path).bam_to_bed())))
                f.writelines(imap(str,
                                  ifilter(standard_chromosomes_filter,
                                          pybedtools.BedTool(reads_b.output().path).bam_to_bed())))

            joint_input_bedtool = pybedtools.BedTool(tf)
            joint_input_bedtool = joint_input_bedtool.sort()

            answer_bedtool = pybedtools.BedTool(consolidated_reads_task.output().path)
            self.assertEqual(joint_input_bedtool.count(), answer_bedtool.count())
            self.assertListEqual(list(joint_input_bedtool), list(answer_bedtool))

class RandomAlignedReads(Task):
    seed = luigi.Parameter()
    length_of_reads = luigi.Parameter()
    number_of_reads = luigi.Parameter()
    genome_version = luigi.Parameter()

    number_of_nonstandard_reads = luigi.Parameter(default=0)

    def _output_directory(self):
        return TestReadConsolidation.task_cache_directory()

    @property
    def _extension(self):
        return 'bam'

    def run(self):
        self.ensure_output_directory_exists()
        abspath = os.path.abspath(self.output().path)
        x = pybedtools.BedTool()
        x = x.random(n=self.number_of_reads, l=self.length_of_reads,
                     seed=self.seed, genome=self.genome_version)

        random = Random(self.seed)
        nonstandard_chromosomes = filter(lambda chrom: '_' in chrom, pybedtools.chromsizes(self.genome_version).keys())

        with temporary_file(suffix='.bed') as bed_tf:
            with open(bed_tf, 'w') as f:
                for i, line in enumerate(x):
                    if i < self.number_of_nonstandard_reads:
                        line.chrom = random.choice(nonstandard_chromosomes)
                        print(str(line))
                    f.write(str(line))
            x = pybedtools.BedTool(bed_tf)
            x = x.to_bam(genome=self.genome_version)
            with temporary_file(cleanup_on_exception=True, suffix='.bam') as bam_tf:
                x.saveas(bam_tf)
                shutil.move(bam_tf, abspath)

        assert os.path.isfile(abspath)
