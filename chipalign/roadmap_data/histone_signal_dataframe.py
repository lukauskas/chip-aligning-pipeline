from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
from functools32 import lru_cache

from chipalign.core.file_formats.dataframe import DataFrameFile
from chipalign.core.task import Task
from chipalign.genome.chromosomes import Chromosomes
from chipalign.roadmap_data.downloaded_signal import DownloadedSignal
from chipalign.roadmap_data.signal_tracks_list import SignalTracksList
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.pandas import BinnedSignalPandas
import chipalign.roadmap_data.settings as roadmap_settings
import pandas as pd
from chipalign.roadmap_data.util import signal_sortkey
from pbinding.mappable_windows import RoadmapMappableBins


@lru_cache(None)
def _histone_binned_signal_tracks(cell_type, binning_method):
    genome_version = roadmap_settings.GENOME_ID
    downloadable_signals = SignalTracksList(genome_version=genome_version,
                                            cell_type=cell_type)

    luigi.build([downloadable_signals])

    bins = RoadmapMappableBins(cell_type)

    tracks = downloadable_signals.output().load()
    ans = {}
    for track in tracks:
        signal = DownloadedSignal(genome_version=genome_version,
                                  cell_type=cell_type,
                                  track=track)

        binned_signal = BinnedSignalPandas(bedgraph_task=BinnedSignal(bins_task=bins,
                                                                      signal_task=signal,
                                                                      binning_method=binning_method
                                                                      ))
        ans[track] = binned_signal

    return ans


class RoadmapHistoneSignal(Task):
    """
    Generates a dataframe of binned signal from ROADMAP data.
    """

    cell_type = luigi.Parameter(default='E008')
    binning_method = BinnedSignal.binning_method

    def _binned_signal_tasks(self):
        return _histone_binned_signal_tracks(cell_type=self.cell_type,
                                             binning_method=self.binning_method)

    @property
    def chromosomes_task(self):
        return Chromosomes(genome_version=self.genome_version,
                           collection='female')

    def requires(self):
        return [self.chromosomes_task] + self._binned_signal_tasks().values()

    @property
    def _extension(self):
        return 'pd'

    @property
    def _output_class(self):
        return DataFrameFile

    def run(self):
        logger = self.logger()

        chromosomes = sorted(self.chromosomes_task.output().load().keys())

        series = []
        for track, task in self._binned_signal_tasks().iteritems():
            input_ = task.output()
            logger.info('Reading {}'.format(input_.path))
            input_series = input_.load()
            input_series.name = track
            # Leave only the chromosomes specified by parameter
            input_series = input_series.loc[chromosomes, :, :]
            series.append(input_series)

        logger.info('Compiling {} series to df'.format(len(series)))
        df = pd.concat(series, axis=1)

        # Sort columns
        logger.info('Sorting')
        df = df[sorted(df.columns, key=signal_sortkey)]
        df = df.sortlevel()

        logger.info('Outputting')
        self.output().dump(df)
