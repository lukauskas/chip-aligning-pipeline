from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
from itertools import izip, groupby
import os
import unittest
import tempfile
import shutil
import luigi
import pybedtools
from chipalign.core.downloader import fetch
from chipalign.genome.genome_alignment import RoadmapAlignedReads, FilteredReads

from tests.roadmap_compatibility.roadmap_tag import roadmap_test
from chipalign.core.util import _CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR

@roadmap_test
class TestTagFiltering(unittest.TestCase):

    _GENOME_VERSION = 'hg19'
    _TAG_LENGTH = 36

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='tests-temp')
        os.environ[_CHIPALIGN_OUTPUT_DIRECTORY_ENV_VAR] = self.temp_dir

        __, self.answer_file = tempfile.mkstemp(prefix='tag-filtering-answer')

        __, gzip_tmp = tempfile.mkstemp(prefix='tag-filtering-answer', suffix='.gz')
        with open(gzip_tmp, 'w') as f:
            fetch('http://egg2.wustl.edu/roadmap/data/byFileType/alignments/'
                  'unconsolidated/Class1Marks/UCSD.H9.H3K56ac.YL238.filt.tagAlign.gz', f)

        with gzip.GzipFile(gzip_tmp, 'r') as _in:
            with open(self.answer_file, 'w') as _out:
                _out.writelines(_in)

        # Slop the bedtool by zeros to truncate the lengths of reads by chromosome boundaries
        # (i.e. their file contains read "chrM 16536 16572" whilst chrM is 16571 bp long)
        # then sort it, and save it to a convenient location
        pybedtools.BedTool(self.answer_file).slop(r=0, l=0, genome=self._GENOME_VERSION).sort().saveas(self.answer_file)

        os.unlink(gzip_tmp)

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_dir)
        except OSError:
            if os.path.isdir(self.temp_dir):
                raise

        try:
            os.unlink(self.answer_file)
        except OSError:
            if os.path.isfile(self.answer_file):
                raise

    def test_h3k56ac_can_be_reproduced(self):
        roadmap_aligned_reads = RoadmapAlignedReads(url='http://genboree.org/EdaccData/Release-9/'
                                                        'experiment-sample/Histone_H3K56ac/H9_Cell_Line/'
                                                        'UCSD.H9.H3K56ac.YL238.bed.gz',
                                                    genome_version=self._GENOME_VERSION)

        filtered_reads_task = FilteredReads(genome_version=self._GENOME_VERSION,
                                            resized_length=self._TAG_LENGTH,
                                            filter_uniquely_mappable_for_truncated_length=True,
                                            remove_duplicates=True,
                                            sort=True,
                                            alignment_task=roadmap_aligned_reads,
                                            chromosomes='male')


        luigi.build([filtered_reads_task], local_scheduler=True)

        self.assertTrue(filtered_reads_task.complete())

        expected = pybedtools.BedTool(self.answer_file)
        actual = pybedtools.BedTool(filtered_reads_task.output().path)

        expected_head = '\n'.join(map(str, expected[:min(10, len(expected))]))
        actual_head = '\n'.join(map(str, actual[:min(10, len(actual))]))

        debug_msg = 'Expected:\n{}\nActual:\n{}\n'.format(expected_head, actual_head)
        self.assertEqual(len(expected), len(actual), 'Length of produced bed files differ: {} != {}.\n'
                                                     '{}'.format(len(expected), len(actual), debug_msg))

        key_func = lambda r: (r.chrom, r.start, r.end)

        prev_key = None
        expected_strand_counts = None
        actual_strand_counts = None

        for expected_row, actual_row in izip(expected, actual):
            # Get the key
            expected_key = key_func(expected_row)
            actual_key = key_func(actual_row)

            # And compare it
            self.assertTupleEqual(expected_key, actual_key)

            # Strand order in datasets is a bit undefined as .sort() by default does not care about them
            # therefore compare them in a sort-invariant way
            if expected_key != prev_key:
                if prev_key is not None:
                    # Check the strand counts match
                    self.assertDictEqual(expected_strand_counts, actual_strand_counts,
                                         'Counts not equal for {!r}: {!r} != {!r}'.format(prev_key,
                                                                                          expected_strand_counts,
                                                                                          actual_strand_counts))
                # Reset counts for new key
                expected_strand_counts = {'+': 0, '-': 0}
                actual_strand_counts = {'+': 0, '-': 0}
                prev_key = expected_key

            # Add the counts
            expected_strand_counts[expected_row.strand] += 1
            actual_strand_counts[actual_row.strand] += 1

        # check the last one
        if prev_key is not None:
            self.assertDictEqual(expected_strand_counts, actual_strand_counts,
                                 'Counts not equal for {!r}: {!r} != {!r}'.format(prev_key,
                                                                                  expected_strand_counts,
                                                                                  actual_strand_counts))