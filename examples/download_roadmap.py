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
import chipalign.database.roadmap_data.downloaded_signal

GENOME_VERSION = 'hg19'


class RoadmapExample(chipalign.core.task.MetaTask):
    cell_type = chipalign.database.roadmap_data.downloaded_signal.DownloadedSignal.cell_type
    track = chipalign.database.roadmap_data.downloaded_signal.DownloadedSignal.track

    def requires(self):
        return chipalign.database.roadmap_data.downloaded_signal.DownloadedSignal(
            cell_type=self.cell_type,
            track=self.track,
            genome_version=GENOME_VERSION)


if __name__ == '__main__':
    import luigi
    luigi.run(main_task_cls=RoadmapExample)
