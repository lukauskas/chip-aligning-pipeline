from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import numpy as np
import tempfile
import shutil
import pandas as pd
from chipalign.core.util import temporary_file


class DataFrameFile(object):

    __HDF_KEY = 'table'

    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path

    def exists(self):
        return os.path.exists(self.path)

    def __dump_hdf(self, df, hdf_filename):
        df.to_hdf(hdf_filename, self.__HDF_KEY)

    def __load_hdf(self, hdf_filename):
        return pd.read_hdf(hdf_filename, self.__HDF_KEY)

    def dump(self, df, verify=True):
        self_path = self.path
        try:
            os.makedirs(os.path.dirname(self_path))
        except OSError:
            if not os.path.isdir(os.path.dirname(self_path)):
                raise

        with temporary_file() as temporary_filename:
            self.__dump_hdf(df, temporary_filename)
            if verify:
                df2 = self.__load_hdf(temporary_filename)
                if not df2.equals(df):
                    raise Exception('DataFrame dump verification failed')
            shutil.move(temporary_filename, self_path)

    def load(self):
        return self.__load_hdf(self.path)
