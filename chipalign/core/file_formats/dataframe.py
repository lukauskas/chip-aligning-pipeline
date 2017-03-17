import os
import datetime
import logging
import shutil
import pandas as pd
from chipalign.core.util import temporary_file


def compress_dataframe(filename_input, filename_output):
    from chipalign.command_line_applications.tables import ptrepack
    logger = logging.getLogger('chipalign.core.file_formats.dataframe.compress_dataframe')
    filesize_original = os.path.getsize(filename_input)

    start_time = datetime.datetime.now()

    # repack while compressing
    ptrepack('--chunkshape', 'auto',
             '--propindexes',
             '--complevel', 1,
             '--complib', 'lzo',
             filename_input, filename_output
             )

    # Report some stats
    end_time = datetime.datetime.now()
    duration = (end_time-start_time).total_seconds()

    filesize_compressed = os.path.getsize(filename_output)
    diff = filesize_original - filesize_compressed
    rel_diff = diff / filesize_original
    logger.debug('DataFrame compression took {:.2f}s and saved {}B ({:.2%})'.format(duration,
                                                                                    diff, rel_diff),
                 extra=dict(filesize_compressed=filesize_compressed,
                            filesize_original=filesize_original,
                            diff=filesize_original - filesize_compressed,
                            duration=duration,
                            rel_diff=rel_diff))


class DataFrameFile(object):

    DEFAULT_HDF_KEY = 'table'

    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path

    def exists(self):
        return os.path.exists(self.path)

    def __dump_hdf(self, df, hdf_filename):
        logger = logging.getLogger('DataFrameFile.__d')

        with temporary_file() as temporary_filename:
            df.to_hdf(temporary_filename, self.DEFAULT_HDF_KEY)
            compress_dataframe(temporary_filename, hdf_filename)

    def __load_hdf(self, hdf_filename):
        return pd.read_hdf(hdf_filename, self.DEFAULT_HDF_KEY)

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

