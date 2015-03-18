from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest
import pybedtools
from genome_alignment import _remove_duplicates_from_bed, _truncate_reads


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


class TestTruncation(unittest.TestCase):
    def sample_data(self, truncate_to_length):
        data = [
            ('chr1', '10', '30', 'a', '99', '+'),
            ('chr1', '18', '68', 'b', '99', '-'),
            ('chr1', '10', '40', 'c', '99', '+'),
            ('chr2', '50', '80', 'h', '99', '-'),
        ]
        input_ = pybedtools.BedTool(data)

        expected_output = [
            ('chr1', '10', 30 - (30-10-truncate_to_length), 'a', '99', '+'),
            ('chr1', 18 + (68-18-truncate_to_length), '68', 'b', '99', '-'),
            ('chr1', '10', 40 - (40-10-truncate_to_length), 'c', '99', '+'),
            ('chr2', (50 + (80-50-truncate_to_length)), '80', 'h', '99', '-'),
        ]
        output_ = pybedtools.BedTool(expected_output)
        return input_, output_


    def test_truncation_to_length_produces_correct_length(self):
        truncation_length = 10

        input_, expected_output = self.sample_data(truncation_length)
        actual_output = _truncate_reads(input_, truncate_to_length=truncation_length)

        lengths = map(lambda x: x.length, actual_output)
        lengths_equal_to_truncation_length = [l == truncation_length for l in lengths]
        self.assertTrue(all(lengths_equal_to_truncation_length), repr(lengths))

    def test_truncation_truncates_at_the_correct_strand(self):
        truncation_length = 10

        input_, expected_output = self.sample_data(truncation_length)
        actual_output = _truncate_reads(input_, truncate_to_length=truncation_length)

        self.assertEqual(expected_output, actual_output,
                         msg="Expected:\n{}\nActual:\n{}".format(expected_output, actual_output))

    def test_truncation_raises_exception_if_reads_already_shorter(self):

        truncation_length = 10

        input_, expected_output = self.sample_data(truncation_length)
        self.assertRaises(Exception, _truncate_reads, input_, 1000)