from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
import pandas as pd

class BedGraph(luigi.File):
    def to_pandas_series(self):
        series = pd.read_table(self.path,
                               header=0, names=['chromosome', 'start', 'end', 'value'],
                               index_col=['chromosome', 'start', 'end'], compression='gzip')

        series = series['value']
        return series


