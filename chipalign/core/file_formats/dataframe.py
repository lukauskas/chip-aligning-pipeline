from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import datetime
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
        from chipalign.command_line_applications.tables import ptrepack

        with temporary_file() as temporary_filename:
            df.to_hdf(temporary_filename, self.__HDF_KEY)

            # repack while compressing
            ptrepack('--chunkshape', 'auto',
                     '--propindexes',
                     '--complevel', 1,
                     '--complib', 'lzo',
                     temporary_filename, hdf_filename
                     )

    def __load_hdf(self, hdf_filename):
        return pd.read_hdf(hdf_filename, self.__HDF_KEY)

    def dump(self, df, verify=True):
        self_path = self.path
        dirname = os.path.dirname(self_path)
        if dirname:
            # would be empty if path is local
            try:
                os.makedirs(dirname)
            except OSError:
                if not os.path.isdir(dirname):
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

    @property
    def modification_time(self):
        if not self.exists():
            return None
        else:
            return datetime.datetime.fromtimestamp(os.path.getmtime(self.path))

