from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from itertools import imap
import os
import luigi
import pybedtools
import shutil
from chipalign.alignment.implementations.consolidation import ConsolidatedReads
from chipalign.core.task import Task
from chipalign.core.util import temporary_file
from tests.helpers.external_resource import ExternalResource
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
                                                    subsample_random_seed=0)
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
                                                    subsample_random_seed=0)
        self.build_task(consolidated_reads_task)

        answer_bedtool = pybedtools.BedTool(consolidated_reads_task.output().path)
        self.assertEqual(max_sequencing_depth, answer_bedtool.count())
        sorted_ = answer_bedtool.sort()
        self.assertEqual(answer_bedtool, sorted_, 'Bedtool that is returned was unsorted')


class RandomAlignedReads(Task):
    seed = luigi.Parameter()
    length_of_reads = luigi.Parameter()
    number_of_reads = luigi.Parameter()
    genome_version = luigi.Parameter()

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
        with temporary_file(suffix='.bed') as bed_tf:
            x.saveas(bed_tf)
            x = x.to_bam(genome=self.genome_version)
            with temporary_file(cleanup_on_exception=True, suffix='.bam') as bam_tf:
                x.saveas(bam_tf)
                shutil.move(bam_tf, abspath)

        assert os.path.isfile(abspath)
