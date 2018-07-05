from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from chipalign.core.util import autocleaning_pybedtools
from chipalign.signal.bins import weighted_means_from_intersection


class TestGenomeWindows(unittest.TestCase):
    sample_data = [('chr1', '9000', '9200', 'chr1', '0', '9971', '0.19923'),
                   ('chr1', '9200', '9400', 'chr1', '0', '9971', '0.19923'),
                   ('chr1', '9400', '9600', 'chr1', '0', '9971', '0.19923'),
                   ('chr1', '9600', '9800', 'chr1', '0', '9971', '0.19923'),
                   ('chr1', '9800', '10000', 'chr1', '0', '9971', '0.19923'),
                   ('chr1', '9800', '10000', 'chr1', '9971', '10048', '0.17932'),
                   ('chr1', '10000', '10200', 'chr1', '9971', '10048', '0.17932'),
                   ('chr1', '10000', '10200', 'chr1', '10050', '10150', '0.4321'),
                   ('chr1', '10200', '10400', '.', '-1', '-1', '.'),
                   ('chr1', '10400', '10600', 'chr1', '10450', '10550', '0.02194'),
                   ('chr2', '10400', '10600', 'chr2', '10550', '10600', '0.01234'),
                   ]

    def expected_output_factory(self, null):
        return [('chr1', 9000, 9200, 0.19923),

                ('chr1', 9200, 9400, 0.19923),
                ('chr1', 9400, 9600, 0.19923),
                ('chr1', 9600, 9800, 0.19923),
                ('chr1', 9800, 10000,
                 (0.19923 * (9971 - 9800) + 0.17932 * (10000 - 9971)) / 200.0),
                ('chr1', 10000, 10200, ((10048 - 10000) * 0.17932
                                        + (10150 - 10050) * 0.4321
                                        + null * (200 - (10048 - 10000) - (10150 - 10050))
                                        ) / 200.0),
                ('chr1', 10200, 10400, null),
                ('chr1', 10400, 10600, (
                    (10550 - 10450) * 0.02194
                    + (200 - (10550 - 10450)) * null
                ) / 200.0),
                ('chr2', 10400, 10600, (
                    (10600 - 10550) * 0.01234
                    + (200 - (10600 - 10550)) * null
                ) / 200.0)]

    def test_weighted_means_from_intersection_general_case(self):
        null_value = 0
        expected_output = self.expected_output_factory(null_value)

        with autocleaning_pybedtools() as pybedtools:
            actual_output = weighted_means_from_intersection(pybedtools.BedTool(self.sample_data), 4, null_value=null_value)
        actual_output = list(actual_output)

        self.assertListEqual(expected_output, actual_output)

    def test_weighted_means_from_intersection_nonzero_null(self):
        null_value = 10000
        expected_output = self.expected_output_factory(null_value)
        with autocleaning_pybedtools() as pybedtools:
            actual_output = weighted_means_from_intersection(pybedtools.BedTool(self.sample_data), 4, null_value=null_value)
        actual_output = list(actual_output)

        self.assertListEqual(expected_output, actual_output)

    def test_different_column_selection_is_fine(self):
        sample_data = self.sample_data
        column_shifted_sample_data = []
        for chr_a, start_a, end_a, chr_b, start_b, end_b, value in sample_data:
            column_shifted_sample_data.append((chr_a, start_a, end_a, chr_b, start_b, end_b, 1, value))

        null_value = 0
        expected_output = self.expected_output_factory(null_value)

        with autocleaning_pybedtools() as pybedtools:
            actual_output = weighted_means_from_intersection(pybedtools.BedTool(column_shifted_sample_data), 5,
                                                             null_value=null_value)
        actual_output = list(actual_output)

        self.assertListEqual(expected_output, actual_output)

    def test_mean_function_can_be_passed(self):
        null_value = 0
        expected_output = self.expected_output_factory(null_value)
        expected_output = [(x[0], x[1], x[2], -5) for x in expected_output]

        with autocleaning_pybedtools() as pybedtools:
            actual_output = weighted_means_from_intersection(pybedtools.BedTool(self.sample_data), 4, null_value=null_value,
                                                             mean_function=lambda __: -5)
        actual_output = list(actual_output)

        self.assertListEqual(expected_output, actual_output)
