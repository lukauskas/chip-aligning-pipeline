from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os

import unittest
import tempfile
import gzip
from StringIO import StringIO
import pybedtools
from chipalign.profile.signal import BinnedSignal, _bedtool_is_sorted
import numpy as np


class TestIsSorted(unittest.TestCase):
    def test_unsorted_bedtool_is_not_sorted(self):
        # The same example as in https://pythonhosted.org/pybedtools/autodocs/pybedtools.BedTool.sort.html
        x = pybedtools.BedTool('''
chr9 300 400
chr1 100 200
chr1 1 50
chr12 1 100
chr9 500 600
''', from_string=True)

        self.assertFalse(_bedtool_is_sorted(x))

    def test_sorted_bedtool_is_sorted(self):
        # The same example as in https://pythonhosted.org/pybedtools/autodocs/pybedtools.BedTool.sort.html
        x = pybedtools.BedTool('''
chr9 300 400
chr1 100 200
chr1 1 50
chr12 1 100
chr9 500 600
''', from_string=True)
        x = x.sort()  # We sort it here

        self.assertTrue(_bedtool_is_sorted(x))


class TestBinnedSignal(unittest.TestCase):
    def test_binned_signal_computes_the_correct_average_p_value(self):

        __, sample_windows_filename = tempfile.mkstemp(suffix='.bed.gz')
        __, sample_signal_filename = tempfile.mkstemp(suffix='.bdg.gz')

        try:
            # Prepare files
            with gzip.GzipFile(sample_windows_filename, 'w') as windows_file:
                windows_file.write('chr1\t4000\t5000\n')
                windows_file.write('chr5\t4000\t8000\n')

            with gzip.GzipFile(sample_signal_filename, 'w') as sample_signal_file:
                # Not overlapping
                sample_signal_file.write('chr1\t2000\t2100\t3\n')

                # Overlapping
                sample_signal_file.write('chr1\t3900\t4100\t0.3\n')
                sample_signal_file.write('chr1\t4300\t4500\t0.5\n')
                sample_signal_file.write('chr1\t4800\t5300\t2\n')

                # Not overlapping
                sample_signal_file.write('chr1\t5300\t5400\t3\n')
                sample_signal_file.write('chr2\t4300\t4500\t3\n')

            s = StringIO()
            BinnedSignal.compute_profile(sample_windows_filename, sample_signal_filename, s)

            expected_score_for_bin = -1 * np.log10(1 / 1000.0 * (100 * np.power(10.0, -0.3)
                                                                 + 200 * np.power(10.0, -0.5)
                                                                 + 200 * np.power(10.0, -2)
                                                                 + (1000 - 200 - 200 - 100) * 1))

            expected_output = 'chr1\t4000\t5000\t{}\nchr5\t4000\t8000\t0.0\n'.format(expected_score_for_bin)

            self.assertEqual(expected_output, s.getvalue())

        finally:
            os.unlink(sample_windows_filename)
            os.unlink(sample_signal_filename)