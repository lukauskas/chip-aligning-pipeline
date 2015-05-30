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
    Pre-parsed bedgraph signal (as provided with `bedgraph_parameter`) to pandas series.
    Reading SignalPandas output is much faster than parsing bedgraph
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
        self.output().dump(series)