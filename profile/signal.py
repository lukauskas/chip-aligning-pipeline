from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import logging
import luigi
from task import Task
from profile.base import compute_profile

import numpy as np

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
        return self.bins_task.parameters + self .signal_task.parameters

    def requires(self):
        return [self.bins_task, self.signal_task]

    @property
    def _extension(self):
        return 'bdg.gz'

    @classmethod
    def compute_profile(cls, bins_abspath, signal_abspath, output_handle):
        logger = cls.logger()
        map_df = compute_profile(bins_abspath,
                                 signal_abspath,
                                 binarise=False,
                                 logger=logger,
                                 operation='weighted_mean',
                                 column=4,
                                 null_value=0,  # 10^(-0) = 1 -> the 'null' p-value
                                 weighted_mean_function=_log10_weighted_mean)

        logger.debug('Mapping done. Length of df: {}. Writing output'.format(len(map_df)))

        for __, row in map_df.iterrows():
            bedgraph_components = [row['chromosome'], row['start'], row['end'], row['value']]
            str_row = '\t'.join(map(str, bedgraph_components))

            output_handle.write(str_row + '\n')


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






