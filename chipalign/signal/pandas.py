from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
from chipalign.core.file_formats.bedgraph import BedGraph
from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.task import Task

class SignalPandas(Task):
    """
    Wrapper around signals in bedgraph format that converts them into pandas objects which
    are then stored in HDF5.

    This is purely for efficiency reasons as reading HDF5 tables is much faster.

    :param bedgraph_task: any task that returns a bedgraph
    """

    bedgraph_task = luigi.Parameter()

    def requires(self):
        assert isinstance(self.bedgraph_task.output(), BedGraph)
        return self.bedgraph_task

    @property
    def _output_class(self):
        return DataFrameFile

    @property
    def _extension(self):
        return 'pd'

    def run(self):
        series = self.input().to_pandas_series()
        series = series.sortlevel()  # To allow for multi-index slicing

        self.output().dump(series)
