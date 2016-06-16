"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python download_roadmap.py --cell-type E008 --track H3K4me3

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""
from __future__ import division
from __future__ import print_function

import chipalign.core.task
from chipalign.database.roadmap.downloaded_signal import RoadmapDownloadedSignal
from chipalign.database.roadmap.mappable_bins import RoadmapMappableBins
from chipalign.signal.bins import BinnedSignal

GENOME_VERSION = 'hg19'


class RoadmapDownloadedBinnedSignal(chipalign.core.task.MetaTask):
    cell_type = RoadmapDownloadedSignal.cell_type
    track = RoadmapDownloadedSignal.track
    binning_method = BinnedSignal.binning_method

    def requires(self):
        signal = RoadmapDownloadedSignal(
            genome_version=GENOME_VERSION)

        bins = RoadmapMappableBins(cell_type=self.cell_type)

        binned_signal = BinnedSignal(bins_task=bins,
                                     signal_task=signal,
                                     binning_method=self.binning_method
                                     )

        return binned_signal

if __name__ == '__main__':
    import luigi

    luigi.run(main_task_cls=RoadmapDownloadedBinnedSignal)
