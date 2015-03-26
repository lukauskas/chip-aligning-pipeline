from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import logging

import luigi
import pybedtools
import numpy as np

from task import Task


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

def _compute_binned_signal(bins_abspath, signal_abspath, output_handle, logger=None,
                           check_sorted=True):
    logger = logger if logger is not None else logging.getLogger('_compute_binned_signal')

    logger.info('Loading bins up and checking if they are sorted')
    bins = pybedtools.BedTool(bins_abspath)
    bins_len = len(bins)
    if check_sorted:
        if not _bedtool_is_sorted(bins):
            raise Exception('Bins need to be sorted')
    logger.debug('Bins length: {}'.format(bins_len))

    logger.info('Loading signal up and checking if they are sorted')
    signal = pybedtools.BedTool(signal_abspath)
    if check_sorted:
        if not _bedtool_is_sorted(signal):
            raise Exception('Signal needs to be sorted')
        logger.debug('Signal length: {}'.format(len(signal)))

    logger.info('Doing the intersection, this takes a while')
    intersection = bins.intersect(signal, loj=True, sorted=True)

    try:
        logger.info('Doing the weighted means thing.')
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

    finally:
        intersection_fn = intersection.fn
        logger.debug('Removing {}'.format(intersection_fn))

        # Remove reference to intersection
        del intersection
        # Delete the file
        try:
            os.unlink(intersection_fn)
        except OSError:
            if os.path.isfile(intersection_fn):
                raise

def _log10_weighted_mean(data):
    weighted_sum = 0
    weights = 0

    for value, weight in data:
        value = np.power(10.0, -value)
        weighted_sum += value * weight
        weights += weight

    weighted_sum /= weights

    score = - np.log10(weighted_sum)
    if score == 0.0:
        return 0.0
    else:
        return score

class BinnedSignal(Task):

    bins_task = luigi.Parameter()
    signal_task = luigi.Parameter()

    @property
    def parameters(self):
        return [self.bins_task.__class__.__name__] + self.bins_task.parameters\
               + [self.signal_task.__class__.__name__] + self.signal_task.parameters

    def requires(self):
        return [self.bins_task, self.signal_task]

    @property
    def _extension(self):
        return 'bdg.gz'

    @classmethod
    def compute_profile(cls, bins_abspath, signal_abspath, output_handle):
        _compute_binned_signal(bins_abspath, signal_abspath, output_handle,
                               logger=cls.class_logger(), check_sorted=False)

    def run(self):
        bins_task_abspath = os.path.abspath(self.bins_task.output().path)
        signal_task_abspath = os.path.abspath(self.signal_task.output().path)

        with self.output().open('w') as f:
            self.compute_profile(bins_task_abspath, signal_task_abspath, f)

if __name__ == '__main__':
    from genome.genome_mappability import FullyMappableGenomicWindows
    from roadmap_data.downloaded_signal import DownloadedSignal
    BinnedSignal.logger().setLevel(logging.DEBUG)
    logging.basicConfig()

    class SignalProfileExample(luigi.Task):
        genome_version = luigi.Parameter(default='hg19')
        chromosomes = luigi.Parameter(default='chr21')

        def requires(self):
            windows = FullyMappableGenomicWindows(genome_version=self.genome_version,
                                                    chromosomes=self.chromosomes,
                                                    read_length=36,
                                                    ext_size=170,
                                                    window_size=200)

            signal = DownloadedSignal(genome_version=self.genome_version,
                                      cell_type='E008',
                                      track='H3K56ac',
                                      chromosomes=self.chromosomes)

            return BinnedSignal(bins_task=windows, signal_task=signal)

    luigi.run(main_task_cls=SignalProfileExample)






