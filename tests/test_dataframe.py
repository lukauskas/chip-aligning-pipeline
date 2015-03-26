from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import os
import tempfile

import pandas as pd

from chipalign.core.file_formats.dataframe import DataFrameFile


class TestDataFrameSaving(unittest.TestCase):

    def test_sample_df(self):
        sample_df = pd.DataFrame({'a': [1, 2, 3], 'b': [14.3, 200, -17]})

        __, tmp_filename = tempfile.mkstemp()
        os.unlink(tmp_filename) # Remove the file, we only need the filename
        try:
            dataframe_file = DataFrameFile(tmp_filename)
            dataframe_file.dump(sample_df)

            roundtrip_df = dataframe_file.load()
            self.assertTrue(sample_df.equals(roundtrip_df), msg="Expected:\n{0}\n{0._data}\n---\nGot:\n{1}\n{1._data}\n".format(sample_df, roundtrip_df))
            self.assertEqual(sample_df.index.names, roundtrip_df.index.names)
        finally:
            try:
                os.unlink(tmp_filename)
            except OSError:
                if os.path.isfile(tmp_filename):
                    raise

    def test_multiindex_df(self):
        sample_df = pd.DataFrame({'a': [1, 2, 3], 'b': [14.3, 200, -17], 'c': [123, 321, 15.4]})
        sample_df = sample_df.set_index(['a', 'c'])

        __, tmp_filename = tempfile.mkstemp()
        os.unlink(tmp_filename)  # Remove the file, we only need the filename
        try:
            dataframe_file = DataFrameFile(tmp_filename)
            dataframe_file.dump(sample_df)

            roundtrip_df = dataframe_file.load()
            self.assertTrue(sample_df.equals(roundtrip_df), msg="Expected:\n{0}\n{0._data}\n---\nGot:\n{1}\n{1._data}\n".format(sample_df, roundtrip_df))
            self.assertEqual(sample_df.index.names, roundtrip_df.index.names)
        finally:
            try:
                os.unlink(tmp_filename)
            except OSError:
                if os.path.isfile(tmp_filename):
                    raise

    def test_named_index_df(self):
        sample_df = pd.DataFrame({'a': [1, 2, 3], 'b': [14.3, 200, -17]})
        sample_df = sample_df.set_index('a')

        __, tmp_filename = tempfile.mkstemp()
        os.unlink(tmp_filename)  # Remove the file, we only need the filename
        try:
            dataframe_file = DataFrameFile(tmp_filename)
            dataframe_file.dump(sample_df)

            roundtrip_df = dataframe_file.load()
            self.assertTrue(sample_df.equals(roundtrip_df), msg="Expected:\n{0}\n{0._data}\n---\nGot:\n{1}\n{1._data}\n".format(sample_df, roundtrip_df))
            self.assertEqual(sample_df.index.names, roundtrip_df.index.names)
        finally:
            try:
                os.unlink(tmp_filename)
            except OSError:
                if os.path.isfile(tmp_filename):
                    raise