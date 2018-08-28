from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import logging

import luigi
import numpy as np

from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.task import Task
from chipalign.core.util import timed_segment, temporary_file, autocleaning_pybedtools
import pandas as pd


class MatrixBinnedSignal(Task):
    """
        Takes the signal and creates a a matrix for all the named windows queried.

        Particularly the signal is binned according to the `binning_method` specified, which
        can be one of the following:

        * `min`: the minimum signal value (note that this is not the minimum p-value
                                           since signal is -log10(p) )
        * `max`: the maximum signal value (again, not a p-value)

        :params bins_task: the task returning bins to compute signal for
        :params signal_task: the actual signal
        :params binning_method: above described binning method to use ('weighted_mean' used here)
    """
    bins_task = luigi.Parameter()
    signal_task = luigi.Parameter()

    binning_method = luigi.Parameter(default='max')

    # _parameter_names_to_hash = ('bins_task', 'signal_task')

    def requires(self):
        return [self.bins_task, self.signal_task]

    @property
    def _extension(self):
        return 'pd'

    @property
    def _output_class(self):
        return DataFrameFile

    @classmethod
    def compute_profile(cls, bins_abspath, signal_abspath, output_handle, pybedtools,
                        method='max'):

        if method in ['max', 'min']:
            _compute_map_signal(bins_abspath, signal_abspath, output_handle,
                                logger=cls.class_logger(),
                                mode=method,
                                pybedtools=pybedtools)
        else:
            raise ValueError('Unsupported method {!r}'.format(method))

    def _run(self):
        logger = self.logger()

        bins_task_abspath = os.path.abspath(self.bins_task.output().path)
        signal_task_abspath = os.path.abspath(self.signal_task.output().path)

        logger.info('Binning signal for {}'.format(signal_task_abspath))

        with autocleaning_pybedtools() as pybedtools:
            with temporary_file() as temp_filename:
                with open(temp_filename, 'w') as f:
                    self.compute_profile(bins_task_abspath, signal_task_abspath, f,
                                         method=self.binning_method,
                                         pybedtools=pybedtools)

                logger.info('Reading signal to pandas dataframe')
                df = pd.read_table(temp_filename,
                                   header=None, names=['chromosome', 'start', 'end', 'name',
                                                       'value'],
                                   index_col=['chromosome', 'start', 'end'])

            with timed_segment("Reshufling dataframe", logger=logger):
                partitioned_name = df['name'].str.rpartition('_')
                df['name'] = partitioned_name[0]
                df['window_id'] = partitioned_name[2].astype(int)
                df = df.set_index(['name', 'window_id'])
                df = df['value']
                df.sort_index(inplace=True)

            logger.info('Dumping output')
            self.output().dump(df)


def _bedtool_is_sorted(bedtool):
    prev = None

    for row in bedtool:
        if prev is not None:
            if row.chrom < prev[0]:
                return False
            elif row.chrom == prev[0] and row.start < prev[1]:
                return False

        prev = (row.chrom, row.start)

    return True


def _compute_map_signal(bins_abspath, signal_abspath, output_handle, pybedtools,
                        logger=None, mode='max'):
    logger = logger if logger is not None else logging.getLogger('_compute_max_signal')

    with timed_segment('Loading data', logger=logger):
        bins = pybedtools.BedTool(bins_abspath)
        signal = pybedtools.BedTool(signal_abspath)

    with timed_segment('Mapping', logger=logger):
        mapped = bins.map(signal, o=mode, c='4', null=0.0)

    with timed_segment('Writing answer'):
        for row in mapped:
            output_handle.write(str(row))

