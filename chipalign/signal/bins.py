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


class BinnedSignal(Task):
    """
        Takes the signal and creates a bedgraph of it distributed across the bins
        specified in `bins_task`.

        Particularly the signal is binned according to the `binning_method` specified, which
        can be one of the following:

        * `min`: the minimum signal value (note that this is not the minimum p-value
                                           since signal is -log10(p) )
        * `max`: the maximum signal value (again, not a p-value)
        * `weighted_mean`: the weighted mean p-value (not signal) where weights is the number
                           of bases covered

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
                        method='weighted_mean'):
        if method == 'weighted_mean':
            _compute_weighted_mean_signal(bins_abspath, signal_abspath, output_handle,
                                          logger=cls.class_logger(), check_sorted=False,
                                          pybedtools=pybedtools)
        elif method in ['max', 'min']:
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
                series = pd.read_table(temp_filename,
                                       header=None, names=['chromosome', 'start', 'end', 'value'],
                                       index_col=['chromosome', 'start', 'end'])

            series = series['value']
            series = series.sortlevel()
            logger.info('Dumping output')
            self.output().dump(series)


def weighted_means_from_intersection(intersection, column, null_value, mean_function=None):

    def _interval_a_grouped_iterator(intersection):
        previous_interval_a = None
        group_ = None

        for row in intersection:
            interval_a = row[:3]
            rest = row[3:]

            if interval_a != previous_interval_a:
                if previous_interval_a is not None:
                    yield previous_interval_a, group_
                previous_interval_a = interval_a
                group_ = []

            group_.append(rest)

        if previous_interval_a is not None:
            yield previous_interval_a, group_

    def _arithmetic_mean(data):
        weighed_sum = 0
        total_weight = 0
        for value, weight in data:
            weighed_sum += value * weight
            total_weight += weight

        return weighed_sum / total_weight

    if mean_function is None:
        mean_function = _arithmetic_mean

    for interval_a, interval_bs in _interval_a_grouped_iterator(intersection):
        chr_a, start_a, end_a = interval_a
        start_a = int(start_a)
        end_a = int(end_a)

        a_length = end_a - start_a

        sum_parts = []

        last_b = None
        for interval_b in interval_bs:
            start_b = int(interval_b[1])
            if start_b == -1:
                continue

            if last_b is not None and start_b < last_b:
                raise Exception('overlapping intervals in b')

            end_b = int(interval_b[2])
            last_b = end_b

            bases_explained_by_b = min(end_a, end_b) - max(start_a, start_b)
            score = float(interval_b[column - 1])
            sum_parts.append((score, bases_explained_by_b))

        unexplained_bases = a_length - sum([w for __, w in sum_parts])
        sum_parts.append((null_value, unexplained_bases))

        yield (chr_a, start_a, end_a, mean_function(sum_parts))

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


def _compute_weighted_mean_signal(bins_abspath, signal_abspath, output_handle,
                                  pybedtools,
                                  logger=None,
                                  check_sorted=True):
    logger = logger if logger is not None else logging.getLogger('_compute_binned_signal')

    with timed_segment('Loading bins up and checking if they are sorted', logger=logger):
        bins = pybedtools.BedTool(bins_abspath)
        bins_len = len(bins)
        if check_sorted:
            if not _bedtool_is_sorted(bins):
                raise Exception('Bins need to be sorted')
        logger.debug('Bins length: {}'.format(bins_len))

    with timed_segment('Loading signal up and checking if they are sorted', logger=logger):
        signal = pybedtools.BedTool(signal_abspath)
        if check_sorted:
            if not _bedtool_is_sorted(signal):
                raise Exception('Signal needs to be sorted')
            logger.debug('Signal length: {}'.format(len(signal)))

    with timed_segment('Doing the intersection, this takes a while', logger=logger):
        intersection = bins.intersect(signal, loj=True, sorted=True)

    with timed_segment('Doing the weighted means thing, this takes a while', logger=logger):
        iter_answer = weighted_means_from_intersection(intersection,
                                                       column=4,
                                                       null_value=0,  # As in 0 = -log_10 (p=1),
                                                       mean_function=_log10_weighted_mean
                                                       )

        rows_written = 0
        # Oh my, answer is an iterator, lets write it as we read it
        for row in iter_answer:
            row = '{}\t{}\t{}\t{}\n'.format(*row)
            output_handle.write(row)
            rows_written += 1
            if rows_written % (bins_len / 10) == 0:
                logging.info('Rows written: {}/{}'.format(rows_written, bins_len))

    logger.info('Done. Total rows written: {}'.format(rows_written))


def _log10_weighted_mean(data):


    values, weights = zip(*data)
    weights = np.array(weights)
    values = np.array(values)

    # Computes weighted average log(sum(w_i * exp(-v_i)) / sum(w_i))
    # using log-sum-exp trick

    min_v = values.min()
    adjusted_values = -values + min_v

    ans = min_v + np.log10(np.sum(weights))
    ans += -np.log10(np.sum(weights * np.power(10.0, adjusted_values)))

    return ans


