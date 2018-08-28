import luigi

from chipalign.alignment.implementations.bowtie.consolidation import ConsolidatedReadsBowtie
from chipalign.core.task import MetaTask
from chipalign.genome.mappability import FullyMappableBins
from chipalign.genome.windows.genome_windows import NonOverlappingBins
from chipalign.genome.windows.summits import WindowsAroundSummits
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.matrixbinnedsignal import MatrixBinnedSignal
from chipalign.signal.peaks import MACSResults
from chipalign.signal.signal import Signal


class MatrixBinnedSignalBowtie(MetaTask):
    """
    A metaclass that creates appropriate matrix binned signal task given the signal task function
    """
    genome_version = NonOverlappingBins.genome_version
    cell_type = ConsolidatedReadsBowtie.cell_type

    read_length = ConsolidatedReadsBowtie.read_length
    binning_method = BinnedSignal.binning_method

    treatment_accessions_str = luigi.Parameter()
    input_accessions_str = luigi.Parameter()

    matrix_accessions_str = luigi.Parameter()
    matrix_slop = WindowsAroundSummits.slop
    matrix_window_size = WindowsAroundSummits.window_size
    matrix_limit = WindowsAroundSummits.limit


    def input_task(self):
        return ConsolidatedReadsBowtie(cell_type=self.cell_type,
                                       genome_version=self.genome_version,
                                       read_length=self.read_length,
                                       accessions_str=self.input_accessions_str)

    def treatment_task(self):
        return ConsolidatedReadsBowtie(accessions_str=self.treatment_accessions_str,
                                       genome_version=self.genome_version,
                                       read_length=self.read_length,
                                       cell_type=self.cell_type)

    def matrix_treatment_task(self):
        return ConsolidatedReadsBowtie(accessions_str=self.matrix_accessions_str,
                                       genome_version=self.genome_version,
                                       read_length=self.read_length,
                                       cell_type=self.cell_type)

    def signal_task(self):
        return Signal(input_task=self.input_task(),
                      treatment_task=self.treatment_task())

    def matrix_macs_task(self):
        return MACSResults(input_task=self.input_task(),
                           treatment_task=self.matrix_treatment_task())

    def bins_task(self):
        return WindowsAroundSummits(genome_version=self.genome_version,
                                    window_size=self.matrix_window_size,
                                    slop=self.matrix_slop,
                                    limit=self.matrix_limit,
                                    macs_task=self.matrix_macs_task())

    def binned_signal(self):
        bins = self.bins_task()
        signal = self.signal_task()

        binned_signal = MatrixBinnedSignal(bins_task=bins,
                                           signal_task=signal,
                                           binning_method=self.binning_method)
        return binned_signal

    def requires(self):
        return self.binned_signal()
