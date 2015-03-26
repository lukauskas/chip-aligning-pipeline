from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging

import luigi
import pandas as pd

from task import Task

from chipalign.core.file_formats.dataframe import DataFrameFile
from genome_signal import Signal
from roadmap_data.downloaded_signal import DownloadableSignalTracks, DownloadedSignal
from genome_alignment import ConsolidatedReads, DownloadedConsolidatedReads
from genome.genome_mappability import FullyMappableGenomicWindows
from profile.signal import BinnedSignal


BRD4_DATA_SRRS = ['SRR1537736', 'SRR1537737']


class BRD4Signal(luigi.Task):
    genome_version = luigi.Parameter(default='hg19')
    aligner = luigi.Parameter(default='pash')
    chromosomes = luigi.Parameter(default='female')
    cell_type = luigi.Parameter(default='E008')

    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth

    @property
    def brd4_consolidated_task(self):
        if self.cell_type == 'E008':
            return ConsolidatedReads(genome_version=self.genome_version,
                                     aligner=self.aligner,
                                     srr_identifiers=BRD4_DATA_SRRS,
                                     chromosomes=self.chromosomes,
                                     max_sequencing_depth=self.max_sequencing_depth)
        else:
            raise ValueError('Unsupported cell type {!r}'.format(self.cell_type))

    @property
    def downloaded_h9_input_task(self):
        return DownloadedConsolidatedReads(genome_version=self.genome_version,
                                           cell_type=self.cell_type,
                                           track='Input',
                                           chromosomes=self.chromosomes
                                           )

    def requires(self):
        return Signal(input_task=self.brd4_consolidated_task, treatment_task=self.downloaded_h9_input_task)

    def output(self):
        return self.requires().output()

    def complete(self):
        return self.requires().complete()

    @property
    def parameters(self):
        return self.requires().parameters

class BRD4Alignments(luigi.Task):
    genome_version = luigi.Parameter()
    aligner = luigi.Parameter(default='pash')
    chromosomes = luigi.Parameter(default='female')

    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth


    def requires(self):
        brd4_signal = ['SRR1537736', 'SRR1537737']  # GSM1466835 BRD4 Vehicle- treated 6h
        brd4_control = ['SRR1537734']  # IgG GSM1466833

        return [ConsolidatedReads(genome_version=self.genome_version,
                                  aligner=self.aligner,
                                  srr_identifiers=brd4_signal,
                                  chromosomes=self.chromosomes,
                                  max_sequencing_depth=self.max_sequencing_depth),
                ConsolidatedReads(genome_version=self.genome_version,
                                  aligner=self.aligner,
                                  srr_identifiers=brd4_control,
                                  chromosomes=self.chromosomes,
                                  max_sequencing_depth=self.max_sequencing_depth)
                ]

    def complete(self):
        return all(map(lambda x: x.complete(), self.requires()))


class H3K56acRefData(luigi.Task):
    genome_version = luigi.Parameter()
    aligner = luigi.Parameter(default='pash')

    chromosomes = luigi.Parameter(default='female')

    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth

    def requires(self):
        srrs = ['SRR179694', 'SRR097968']

        return [ConsolidatedReads(genome_version=self.genome_version,
                                  aligner=self.aligner,
                                  srr_identifiers=srrs,
                                  chromosomes=self.chromosomes,
                                  max_sequencing_depth=self.max_sequencing_depth)]

    def complete(self):
        return all(map(lambda x: x.complete(), self.requires()))


class SignalDataFrame(Task):
    genome_version = luigi.Parameter(default='hg19')
    chromosomes = luigi.Parameter(default='female')
    cell_type = luigi.Parameter(default='E008')

    ext_size = luigi.IntParameter(default=170)
    window_size = luigi.IntParameter(default=200)
    read_length = luigi.IntParameter(default=36)

    aligner = BRD4Signal.aligner

    max_sequencing_depth = BRD4Signal.max_sequencing_depth

    def _possible_signal_tracks(self):
        try:
            return self.__signals_cache
        except AttributeError:
            downloadable_signals = DownloadableSignalTracks(genome_version=self.genome_version,
                                                            cell_type=self.cell_type)
            luigi.build([downloadable_signals])

            tracks = downloadable_signals.output().load()
            signals = {track: DownloadedSignal(genome_version=self.genome_version, cell_type=self.cell_type,
                                               chromosomes=self.chromosomes, track=track) for track in tracks}

            brd4_signal = BRD4Signal(genome_version=self.genome_version,
                                     aligner=self.aligner,
                                     chromosomes=self.chromosomes,
                                     max_sequencing_depth=self.max_sequencing_depth,
                                     )

            signals['BRD4'] = brd4_signal

            self.__signals_cache = signals

            return self.__signals_cache


    def _binned_signal_tasks(self):
        signals = self._possible_signal_tracks()

        mappable_windows = FullyMappableGenomicWindows(genome_version=self.genome_version,
                                                       chromosomes=self.chromosomes,
                                                       read_length=self.read_length,
                                                       ext_size=self.ext_size,
                                                       window_size=self.window_size)

        binned_signals = {track: BinnedSignal(bins_task=mappable_windows, signal_task=signal)
                          for track, signal in signals.iteritems()}

        return binned_signals


    def requires(self):
        return self._binned_signal_tasks().values()


    @property
    def _extension(self):
        return 'df'


    @property
    def parameters(self):
        return [self.genome_version, self.cell_type,
                'w{}'.format(self.window_size), 'e{}'.format(self.ext_size),
                'r{}'.format(self.read_length), self.chromosomes]


    def output(self):
        path = super(SignalDataFrame, self).output().path

        return DataFrameFile(path)


    def run(self):
        logger = self.logger()

        series = []
        for track, task in self._binned_signal_tasks().iteritems():
            input_ = task.output()
            logger.info('Reading {}'.format(input_.path))
            input_series = pd.read_table(input_.path,
                                         header=0, names=['chromosome', 'start', 'end', 'p_value'],
                                         index_col=['chromosome', 'start', 'end'], compression='gzip')

            input_series = input_series['p_value']
            input_series.name = track

            series.append(input_series)

        logger.info('Compiling {} series to df'.format(len(series)))
        df = pd.concat(series, axis=1)
        logger.info('Outputting')
        self.output().dump(df)


class SignalTest(Task):
    genome_version = luigi.Parameter(default='hg19')
    cell_type = luigi.Parameter(default='E008')
    track = luigi.Parameter(default='H3K56ac')
    chromosomes = luigi.Parameter(default='female')

    def requires(self):
        input_reads = DownloadedConsolidatedReads(genome_version=self.genome_version,
                                                  cell_type=self.cell_type,
                                                  track='Input',
                                                  chromosomes=self.chromosomes
                                                  )

        track_reads = DownloadedConsolidatedReads(genome_version=self.genome_version,
                                                  cell_type=self.cell_type,
                                                  track=self.track,
                                                  chromosomes=self.chromosomes
                                                  )

        return Signal(input_task=input_reads, treatment_task=track_reads)

    def complete(self):
        return self.requires().complete()


if __name__ == '__main__':
    logging.basicConfig()
    luigi.run()
