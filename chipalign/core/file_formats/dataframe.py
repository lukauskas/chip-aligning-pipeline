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

class _Legacy_DataFrameFile(object):

    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path

    def exists(self):
        return os.path.exists(self.path)

    def dump(self, df, verify=True):
        self_path = self.path
        try:
            os.makedirs(os.path.dirname(self_path))
        except OSError:
            if not os.path.isdir(os.path.dirname(self_path)):
                raise

        __, tmp_file = tempfile.mkstemp(prefix=self.path, suffix='.npz', dir='.')
        try:
            payload = {'columns': np.array(df.columns),
                       'index': np.array(df.index),
                       'values': df.values,
                       'dtypes': np.array(df.dtypes),
                       'index_names': np.array(df.index.names)}
            if isinstance(df.index, pd.MultiIndex):
                payload['multi_index'] = True

            np.savez(tmp_file, **payload)

            if verify:
                loaded_payload = np.load(tmp_file)
                if len(loaded_payload.keys()) != len(payload.keys()):
                    raise IOError('Something went wrong writing the payload')
                else:
                    for key, value in payload.iteritems():

                        if isinstance(value, np.ndarray):
                            not_equal = not np.array_equal(value, loaded_payload[key])
                        else:
                            not_equal = value != loaded_payload[key]

                        if not_equal:
                            raise IOError('Something went wrong when writing the payload. Keys {} don\'t match'.format(key))

            shutil.move(tmp_file, self_path)
        finally:
            try:
                os.unlink(tmp_file)
            except OSError:
                if os.path.isfile(tmp_file):
                    raise

    def load(self):
        data = np.load(self.path)
        df = pd.DataFrame(data['values'], index=data['index'], columns=data['columns'])

        for column, dtype in zip(data['columns'], data['dtypes']):
            df[column] = df[column].astype(dtype)

        try:
            multi_index = data['multi_index']
        except KeyError:
            multi_index = None

        if multi_index:
            df.index = pd.MultiIndex.from_tuples(df.index, names=data['index_names'])
        else:
            df.index.names = data['index_names']

        return df
