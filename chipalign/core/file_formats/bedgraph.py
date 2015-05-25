from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi.format

import pandas as pd
from chipalign.core.file_formats.file import File


class BedGraph(File):
    def __init__(self, path=None, **kwargs):
        if path.endswith('gz'):
            format_ = kwargs.get('format', luigi.format.Gzip)
        else:
            format_ = kwargs.get('format', None)

        super(BedGraph, self).__init__(path=path, format=format_, **kwargs)

    def to_pandas_series(self):
        series = pd.read_table(self.path,
                               header=0, names=['chromosome', 'start', 'end', 'value'],
                               index_col=['chromosome', 'start', 'end'], compression='gzip')

        series = series['value']
        return series


