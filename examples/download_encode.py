"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python download_encode.py --cell-type E123 --accession ENCFF077EXO

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""
from __future__ import division
from __future__ import print_function

import chipalign.core.task
from chipalign.database.encode.downloaded_signal import EncodeDownloadedSignal
from chipalign.database.roadmap.mappable_bins import RoadmapMappableBins
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.pandas import BinnedSignalPandas

GENOME_VERSION = 'hg19'


class EncodeDownloadedBinnedSignal(chipalign.core.task.MetaTask):
    cell_type = RoadmapMappableBins.cell_type
    accession = EncodeDownloadedSignal.accession
    binning_method = BinnedSignal.binning_method

    def requires(self):
        signal = EncodeDownloadedSignal(accession=self.accession)
        bins = RoadmapMappableBins(cell_type=self.cell_type)

        binned_signal = BinnedSignalPandas(bedgraph_task=BinnedSignal(bins_task=bins,
                                                                      signal_task=signal,
                                                                      binning_method=self.binning_method
                                                                      ))

        return binned_signal


if __name__ == '__main__':
    import luigi
    luigi.run(main_task_cls=EncodeDownloadedBinnedSignal)
