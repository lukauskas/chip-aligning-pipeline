from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import logging
import luigi
import pybedtools
from task import Task
from profile.base import weighted_means_from_intersection
import numpy as np

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
    from genome_mappability import FullyMappableGenomicWindows
    from downloaded_signal import DownloadedSignal
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






