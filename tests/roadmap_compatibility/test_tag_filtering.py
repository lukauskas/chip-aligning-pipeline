from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import gzip
from itertools import izip
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
                                                    genome_version='hg19')

        filtered_reads_task = FilteredReads(genome_version='hg19',
                                            resized_length=36,
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


        for expected_row, actual_row in izip(expected, actual):
            row_message = 'Row expected:\n{}\nRow actual:\n{}\n'.formaT(expected_row, actual_row)
            mismatch_message = 'Line by line differences:\n{}\n\n{}'.format(row_message, debug_msg)

            self.assertEqual(expected_row.chrom, actual_row.chrom, mismatch_message)
            self.assertEqual(expected_row.start, actual_row.start, mismatch_message)
            self.assertEqual(expected_row.end, actual_row.end, mismatch_message)
            self.assertEqual(expected_row.strand, actual_row.strand, mismatch_message)
