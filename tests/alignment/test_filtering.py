from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_frame_equal

from chipalign.alignment.filtering import _remove_duplicate_reads_inplace, _resize_reads_inplace

import pybedtools

class TestFiltering(unittest.TestCase):

    def sample_data(self):
        data = [
            ('chr1', '10', '30', 'duplicate-a1', '99', '+'),
            ('chr1', '18', '68', 'neg-strand-duplicate-a1', '99', '-'),
            ('chr1', '10', '40', 'duplicate-a2', '99', '+'),
            ('chr1', '10', '35', 'non-duplicate-to-a', '99', '-'),
            ('chr2', '10', '30', 'non-duplicate-to-a-as-chr2', '99', '+'),
            ('chr1', '16', '68', 'neg-strand-duplicate-a2', '99', '-'),
            ('chr1', '50', '80', 'non-duplicate-to-a-as-different-loc', '99', '+'),
            ('chr1', '50', '80', 'non-duplicate-to-b-as-different-loc-neg', '99', '-'),
        ]
        input_ = pybedtools.BedTool(data)

        expected_output = [
            ('chr1', '10', '35', 'non-duplicate-to-a', '99', '-'),
            ('chr2', '10', '30', 'non-duplicate-to-a-as-chr2', '99', '+'),
            ('chr1', '50', '80', 'non-duplicate-to-a-as-different-loc', '99', '+'),
            ('chr1', '50', '80', 'non-duplicate-to-b-as-different-loc-neg', '99', '-'),
        ]
        output_ = pybedtools.BedTool(expected_output)
        return input_, output_

    def test_duplicate_filtering_works(self):
        input_, expected_output = self.sample_data()

        input_ = input_.to_dataframe()
        expected_output = expected_output.to_dataframe()

        output_ = input_.copy()
        _remove_duplicate_reads_inplace(output_)

        expected_output_sorted = expected_output.sort_values(by=['chrom', 'start', 'end', 'strand'])
        expected_output_sorted = expected_output_sorted[['chrom', 'start', 'end', 'strand']]

        output_sorted = output_.sort_values(by=['chrom', 'start', 'end', 'strand'])
        output_sorted = output_sorted[['chrom', 'start', 'end', 'strand']]

        assert_array_equal(expected_output_sorted.values, output_sorted.values)
        self.assertTrue(output_.columns.equals(expected_output.columns))


class TestResizing(unittest.TestCase):
    def test_resizing_produces_correct_length(self):
        data = [
            # Need to shorten
            ('chr1', '10', '30', 'a', '99', '+'),  # Length: 20
            ('chr1', '18', '68', 'b', '99', '-'),  # Length: 50
            ('chr1', '10', '40', 'c', '99', '+'),  # Length: 30
            ('chr2', '50', '80', 'h', '99', '-'),  # Length: 30
            # Need to extend
            ('chr1', '10', '15', 'e', '99', '+'),  # Length: 5
            ('chr2', '50', '60', 'f', '99', '-')  # Length: 10
        ]
        input_ = pybedtools.BedTool(data).to_dataframe()

        # Large enough so we do not need to worry about boundaries
        chromsizes = {'chr1': (0, 300), 'chr2': (0, 300)}

        new_length = 20
        _resize_reads_inplace(input_, new_length=new_length,
                              chromsizes=chromsizes,
                              )

        lengths = (input_.end - input_.start).abs()
        lengths_equal_to_truncation_length = [l == new_length for l in lengths]
        self.assertTrue(all(lengths_equal_to_truncation_length), repr(lengths))

    def test_resizing_works_on_correct_strand(self):

        data = [
            # Need to shorten
            ('chr1', '10', '30', 'a', '99', '+'),  # Length: 20
            ('chr1', '18', '68', 'b', '99', '-'),  # Length: 50
            ('chr1', '10', '40', 'c', '99', '+'),  # Length: 30
            ('chr2', '50', '80', 'h', '99', '-'),  # Length: 30
            # Need to extend
            ('chr1', '10', '15', 'e', '99', '+'),  # Length: 5
            ('chr2', '50', '60', 'f', '99', '-')  # Length: 10
        ]

        new_length = 20

        new_data = [
            # Need to shorten
            ('chr1', '10', '30', 'a', '99', '+'),  # Length: 20
            ('chr1', '48', '68', 'b', '99', '-'),  # Length: 20
            ('chr1', '10', '30', 'c', '99', '+'),  # Length: 20
            ('chr2', '60', '80', 'h', '99', '-'),  # Length: 20
            # Need to extend
            ('chr1', '10', '30', 'e', '99', '+'),  # Length: 5
            ('chr2', '40', '60', 'f', '99', '-')  # Length: 10
        ]

        input_ = pybedtools.BedTool(data).to_dataframe()
        expected_output = pybedtools.BedTool(new_data).to_dataframe()

        # Large enough so we do not need to worry about boundaries
        chromsizes = {'chr1': (0, 300), 'chr2': (0, 300)}

        _resize_reads_inplace(input_, new_length=new_length,
                              chromsizes=chromsizes)

        assert_frame_equal(expected_output, input_)

    def test_resizing_raises_exception_if_needed(self):

        shorten_data = [
            # Need to shorten
            ('chr1', '18', '68', 'b', '99', '-'),  # Length: 50
            ('chr1', '10', '40', 'c', '99', '+'),  # Length: 30
            ('chr2', '50', '80', 'h', '99', '-'),  # Length: 30
        ]
        extend_data = [
            # Need to extend
            ('chr1', '10', '15', 'e', '99', '+'),  # Length: 5
            ('chr2', '50', '60', 'f', '99', '-')  # Length: 10
        ]

        new_length = 20

        # Large enough so we do not need to worry about boundaries
        chromsizes = {'chr1': (0, 300), 'chr2': (0, 300)}

        try:
            _resize_reads_inplace(pybedtools.BedTool(shorten_data).to_dataframe(),
                                  new_length, chromsizes=chromsizes,
                                  )
        except Exception as e:
            self.fail('Unexpected exception {!r}'.format(e))

        self.assertRaises(Exception,
                          _resize_reads_inplace,
                          pybedtools.BedTool(shorten_data + [extend_data[0]]),
                          new_length, chromsizes=chromsizes,
                          pybedtools=pybedtools
                          )

        try:
            _resize_reads_inplace(pybedtools.BedTool(extend_data).to_dataframe(),
                                  new_length, chromsizes=chromsizes)
        except Exception as e:
            self.fail('Unexpected exception {!r}'.format(e))

    def test_resizing_respects_chromosome_bounds(self):

        data = [
            # Need to extend
            ('chr1', '0', '30', 'a', '99', '+'),  # Length: 30
            ('chr2', '5', '25', 'h', '99', '-'),  # Length: 20
        ]

        new_length = 50

        new_data = [
            ('chr1', '0', '35', 'a', '99', '+'),  # Length: 35
            ('chr2', '2', '25', 'h', '99', '-'),  # Length: 23
        ]

        # Specific, and rather unusual boundaries
        chromsizes = {'chr1': (0, 35), 'chr2': (2, 300)}

        input_ = pybedtools.BedTool(data).to_dataframe()
        expected_output = pybedtools.BedTool(new_data).to_dataframe()

        _resize_reads_inplace(input_, new_length=new_length,
                              chromsizes=chromsizes)

        assert_frame_equal(expected_output, input_)
