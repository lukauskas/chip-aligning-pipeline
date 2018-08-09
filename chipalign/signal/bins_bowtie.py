import luigi

from chipalign.alignment.implementations.bowtie.consolidation import ConsolidatedReadsBowtie
from chipalign.core.task import MetaTask
from chipalign.genome.mappability import FullyMappableBins
from chipalign.genome.windows.genome_windows import NonOverlappingBins
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.signal import Signal


class BinnedSignalBowtie(MetaTask):
    """
    A metaclass that creates appropriate binned signal task given the signal task function
    """
    genome_version = NonOverlappingBins.genome_version
    cell_type = ConsolidatedReadsBowtie.cell_type

    window_size = NonOverlappingBins.window_size
    remove_blacklisted = NonOverlappingBins.remove_blacklisted
    read_length = ConsolidatedReadsBowtie.read_length
    binning_method = BinnedSignal.binning_method
    max_ext_size = FullyMappableBins.max_ext_size

    treatment_accessions_str = luigi.Parameter()
    input_accessions_str = luigi.Parameter()

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

    def signal_task(self):
        return Signal(input_task=self.input_task(),
                      treatment_task=self.treatment_task())

    def bins_task(self):
        bins = NonOverlappingBins(genome_version=self.genome_version,
                                  window_size=self.window_size,
                                  remove_blacklisted=self.remove_blacklisted)

        return FullyMappableBins(bins_task=bins,
                                 max_ext_size=self.max_ext_size,
                                 read_length=self.read_length)

    def binned_signal(self):
        bins = self.bins_task()
        signal = self.signal_task()

        binned_signal = BinnedSignal(bins_task=bins,
                                     signal_task=signal,
                                     binning_method=self.binning_method)
        return binned_signal

    def requires(self):
        return self.binned_signal()
