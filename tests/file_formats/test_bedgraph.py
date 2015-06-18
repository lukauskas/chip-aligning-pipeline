from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from contextlib import contextmanager
import gzip

import unittest
from chipalign.core.file_formats.bedgraph import BedGraph
from chipalign.core.util import temporary_file
import pandas as pd
from pandas.util.testing import assert_series_equal

@contextmanager
def _temporary_bedgraph(contents, gzipped=False):

    suffix = '.gz' if gzipped else ''
    with temporary_file(suffix=suffix) as tf:

        open_ = gzip.GzipFile if gzipped else open

        with open_(tf, 'w') as f:
            f.write(contents)

        yield BedGraph(tf)


class TestBedGraphReading(unittest.TestCase):

    def test_sample_bedgraph_is_read_correctly(self):

        contents = u"chr1\t0\t9959\t0.22042\n" \
                   u"chr1\t9959\t10359\t0.19921\n" \
                   u"chr1\t10359\t10361\t0.22042\n" \
                   u"chr1\t10361\t10438\t0.19921\n"

        expected_series = pd.DataFrame({'chromosome': ['chr1', 'chr1', 'chr1', 'chr1'],
                                        'start': [0, 9959, 10359, 10361],
                                        'end': [9959, 10359, 10361, 10438],
                                        'value': [0.22042, 0.19921, 0.22042, 0.19921]})
        expected_series = expected_series.set_index(['chromosome', 'start', 'end'])
        expected_series = expected_series['value']
        expected_series.name = None  # No name specified in bedgraph track

        with _temporary_bedgraph(contents) as bedgraph:
            self.assertTrue(bedgraph.exists())
            bedgraph_as_series = bedgraph.to_pandas_series()

            assert_series_equal(expected_series, bedgraph_as_series,
                                check_series_type=True,
                                check_names=True)
            self.assertEqual(expected_series.name, bedgraph_as_series.name)

    def test_sample_bedgraph_ame_can_be_overriden(self):

        contents = u"chr1\t0\t9959\t0.22042\n" \
                   u"chr1\t9959\t10359\t0.19921\n" \
                   u"chr1\t10359\t10361\t0.22042\n" \
                   u"chr1\t10361\t10438\t0.19921\n"

        expected_series = pd.DataFrame({'chromosome': ['chr1', 'chr1', 'chr1', 'chr1'],
                                        'start': [0, 9959, 10359, 10361],
                                        'end': [9959, 10359, 10361, 10438],
                                        'value': [0.22042, 0.19921, 0.22042, 0.19921]})
        expected_series = expected_series.set_index(['chromosome', 'start', 'end'])
        expected_series = expected_series['value']
        expected_series.name = 'foobar'

        with _temporary_bedgraph(contents) as bedgraph:
            self.assertTrue(bedgraph.exists())
            bedgraph_as_series = bedgraph.to_pandas_series(name='foobar')

            assert_series_equal(expected_series, bedgraph_as_series,
                                check_series_type=True,
                                check_names=True)
            self.assertEqual(expected_series.name, bedgraph_as_series.name)

    def test_sample_gzipped_bedgraph_is_read_correctly(self):

        contents = u"chr1\t0\t9959\t0.22042\n" \
                   u"chr1\t9959\t10359\t0.19921\n" \
                   u"chr1\t10359\t10361\t0.22042\n" \
                   u"chr1\t10361\t10438\t0.19921\n"

        expected_series = pd.DataFrame({'chromosome': ['chr1', 'chr1', 'chr1', 'chr1'],
                                        'start': [0, 9959, 10359, 10361],
                                        'end': [9959, 10359, 10361, 10438],
                                        'value': [0.22042, 0.19921, 0.22042, 0.19921]})
        expected_series = expected_series.set_index(['chromosome', 'start', 'end'])
        expected_series = expected_series['value']
        expected_series.name = None  # No name specified in bedgraph track

        with _temporary_bedgraph(contents, gzipped=True) as bedgraph:
            self.assertTrue(bedgraph.exists())
            bedgraph_as_series = bedgraph.to_pandas_series()

            assert_series_equal(expected_series, bedgraph_as_series,
                                check_series_type=True,
                                check_names=True)
            self.assertEqual(expected_series.name, bedgraph_as_series.name)

    def test_sample_bedgraph_with_header_is_read_correctly(self):

        contents = u'track type="bedGraph" name="foobar" description="description with spaces"\n' \
                   u'chr1\t0\t9959\t0.22042\n' \
                   u"chr1\t9959\t10359\t0.19921\n" \
                   u"chr1\t10359\t10361\t0.22042\n" \
                   u"chr1\t10361\t10438\t0.19921\n"

        expected_series = pd.DataFrame({'chromosome': ['chr1', 'chr1', 'chr1', 'chr1'],
                                        'start': [0, 9959, 10359, 10361],
                                        'end': [9959, 10359, 10361, 10438],
                                        'value': [0.22042, 0.19921, 0.22042, 0.19921]})
        expected_series = expected_series.set_index(['chromosome', 'start', 'end'])
        expected_series = expected_series['value']
        expected_series.name = 'foobar'  # Should be the same as bedgraph tracks name

        with _temporary_bedgraph(contents) as bedgraph:
            self.assertTrue(bedgraph.exists())
            bedgraph_as_series = bedgraph.to_pandas_series()

            assert_series_equal(expected_series, bedgraph_as_series,
                                check_series_type=True,
                                check_names=True)
            self.assertEqual(expected_series.name, bedgraph_as_series.name)

    def test_sample_bedgraph_with_header_is_read_correctly_from_gzip(self):
        contents = u'track type="bedGraph" name="foobar" description="description with spaces"\n' \
                   u"chr1\t0\t9959\t0.22042\n" \
                   u"chr1\t9959\t10359\t0.19921\n" \
                   u"chr1\t10359\t10361\t0.22042\n" \
                   u"chr1\t10361\t10438\t0.19921\n"

        expected_series = pd.DataFrame({'chromosome': ['chr1', 'chr1', 'chr1', 'chr1'],
                                        'start': [0, 9959, 10359, 10361],
                                        'end': [9959, 10359, 10361, 10438],
                                        'value': [0.22042, 0.19921, 0.22042, 0.19921]})
        expected_series = expected_series.set_index(['chromosome', 'start', 'end'])
        expected_series = expected_series['value']
        expected_series.name = 'foobar'  # Should be the same as bedgraph tracks name

        with _temporary_bedgraph(contents, gzipped=True) as bedgraph:
            self.assertTrue(bedgraph.exists())
            bedgraph_as_series = bedgraph.to_pandas_series()

            assert_series_equal(expected_series, bedgraph_as_series,
                                check_series_type=True,
                                check_names=True)
            self.assertEqual(expected_series.name, bedgraph_as_series.name)

    def test_sample_bedgraph_header_returns_empty_dict_if_no_header_present(self):
        contents = u"chr1\t0\t9959\t0.22042\n"

        expected_header = {}

        with _temporary_bedgraph(contents, gzipped=True) as bedgraph:
            actual_header = bedgraph.header()

        with _temporary_bedgraph(contents, gzipped=True) as gzipped_bedgraph:
            actual_header_gzipped = gzipped_bedgraph.header()

        self.assertDictEqual(expected_header, actual_header)
        self.assertDictEqual(expected_header, actual_header_gzipped)

    def test_sample_bedgraph_header_is_parsed_correctly(self):
        contents = u'track type="bedGraph" name=foobar description="description with spaces"\n' \
                   u"chr1\t0\t9959\t0.22042\n"

        expected_header = {'type': 'bedGraph', 'name': 'foobar', 'description': 'description with spaces'}

        with _temporary_bedgraph(contents, gzipped=True) as bedgraph:
            actual_header = bedgraph.header()

        with _temporary_bedgraph(contents, gzipped=True) as gzipped_bedgraph:
            actual_header_gzipped = gzipped_bedgraph.header()

        self.assertDictEqual(expected_header, actual_header)
        self.assertDictEqual(expected_header, actual_header_gzipped)