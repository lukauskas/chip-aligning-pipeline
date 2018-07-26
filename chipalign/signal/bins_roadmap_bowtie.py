import luigi

from chipalign.alignment.implementations.bowtie.consolidation import ConsolidatedReadsBowtie
from chipalign.core.task import MetaTask
from chipalign.database.roadmap.mappable_bins import RoadmapMappableBins
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.signal import Signal


class BinnedSignalRoadmapBowtie(MetaTask):
    """
    A metaclass that creates appropriate binned signal task given the signal task function
    """
    genome_version = RoadmapMappableBins.genome_version
    cell_type = RoadmapMappableBins.cell_type
    binning_method = BinnedSignal.binning_method

    treatment_accessions_str = luigi.Parameter()
    input_accessions_str = luigi.Parameter()

    def input_task(self):
        return ConsolidatedReadsBowtie(cell_type=self.cell_type,
                                       genome_version=self.genome_version,
                                       accessions_str=self.input_accessions_str)

    def treatment_task(self):
        return ConsolidatedReadsBowtie(accessions_str=self.treatment_accessions_str,
                                       genome_version=self.genome_version,
                                       cell_type=self.cell_type)

    def signal_task(self):
        return Signal(input_task=self.input_task(),
                      treatment_task=self.treatment_task())

    def bins_task(self):
        return RoadmapMappableBins(genome_version=self.genome_version,
                                   cell_type=self.cell_type)

    def binned_signal(self):
        bins = self.bins_task()
        signal = self.signal_task()

        binned_signal = BinnedSignal(bins_task=bins,
                                     signal_task=signal,
                                     binning_method=self.binning_method
                                     )
        return binned_signal

    def requires(self):
        return self.binned_signal()