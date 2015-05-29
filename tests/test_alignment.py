from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest

import pybedtools

from chipalign.genome.genome_alignment import _resize_reads
from chipalign.alignment.filtering import _remove_duplicates_from_bed, _resize_reads


class TestFiltering(unittest.TestCase):
    def sample_data(self):
        data = [
            ('chr1', '10', '30', 'duplicate-a1', '99', '+'),
            ('chr1', '18', '68', 'neg-strand-duplicate-a1', '99', '-'),
            ('chr1', '10', '40', 'duplicate-a2', '99', '+'),
            ('chr1', '10', '35', 'non-duplicate-to-a', '99', '-'),
            ('chr2', '10', '30', 'non-duplicate-to-a-as-chr2', '99', '+'),
            ('chr1', '16', '68', 'neg-strand-duplicate-a2', '99', '-'),
            ('chr1', '50', '80', 'non-duplicate-to-a-as different loc', '99', '+'),
            ('chr1', '50', '80', 'non-duplicate-to-b-as different loc-neg', '99', '-'),
        ]
        input_ = pybedtools.BedTool(data)

        expected_output = [
            ('chr1', '10', '35', 'non-duplicate-to-a', '99', '-'),
            ('chr2', '10', '30', 'non-duplicate-to-a-as-chr2', '99', '+'),
            ('chr1', '50', '80', 'non-duplicate-to-a-as different loc', '99', '+'),
            ('chr1', '50', '80', 'non-duplicate-to-b-as different loc-neg', '99', '-'),
        ]
        output_ = pybedtools.BedTool(expected_output)
        return input_, output_


    def test_duplicate_filtering_works(self):
        input_, expected_output = self.sample_data()

        output_ = _remove_duplicates_from_bed(input_)

        self.assertEqual(expected_output, output_)


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
            ('chr2', '50', '60', 'f', '99', '-')   # Length: 10
        ]
        input_ = pybedtools.BedTool(data)

        # Large enough so we do not need to worry about boundaries
        chromsizes = {'chr1': (0, 300), 'chr2': (0, 300)}

        new_length = 20

        actual_output = _resize_reads(input_, new_length=new_length,
                                      chromsizes=chromsizes,
                                      can_shorten=True,
                                      can_extend=True)

        lengths = map(lambda x: x.length, actual_output)
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
            ('chr2', '50', '60', 'f', '99', '-')   # Length: 10
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
            ('chr2', '40', '60', 'f', '99', '-')   # Length: 10
        ]

        input_ = pybedtools.BedTool(data)
        expected_output = pybedtools.BedTool(new_data)

        # Large enough so we do not need to worry about boundaries
        chromsizes = {'chr1': (0, 300), 'chr2': (0, 300)}

        actual_output = _resize_reads(input_, new_length=new_length,
                                      chromsizes=chromsizes,
                                      can_shorten=True,
                                      can_extend=True)

        self.assertEqual(expected_output, actual_output,
                         msg="Expected:\n{}\nActual:\n{}".format(expected_output, actual_output))


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
            _resize_reads(pybedtools.BedTool(shorten_data),
                          new_length, chromsizes=chromsizes,
                          can_shorten=True,
                          can_extend=False)
        except Exception as e:
            self.fail('Unexpected exception {!r}'.format(e))

        self.assertRaises(Exception,
                          _resize_reads,
                          pybedtools.BedTool(shorten_data + [extend_data[0]]),
                          new_length, chromsizes=chromsizes,
                          can_shorten=True,
                          can_extend=False
                          )

        try:
            _resize_reads(pybedtools.BedTool(extend_data),
                          new_length, chromsizes=chromsizes,
                          can_shorten=False,
                          can_extend=True)
        except Exception as e:
            self.fail('Unexpected exception {!r}'.format(e))

        self.assertRaises(Exception,
                          _resize_reads,
                          pybedtools.BedTool(extend_data + [shorten_data[0]]),
                          new_length, chromsizes=chromsizes,
                          can_shorten=False,
                          can_extend=True
                          )

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

        input_ = pybedtools.BedTool(data)
        expected_output = pybedtools.BedTool(new_data)

        actual_output = _resize_reads(input_, new_length=new_length,
                                      chromsizes=chromsizes,
                                      can_shorten=True,
                                      can_extend=True)

        self.assertEqual(expected_output, actual_output,
                         msg="Expected:\n{}\nActual:\n{}".format(expected_output, actual_output))
