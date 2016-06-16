"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python fully_mappable_bins.py --cell-type E008

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""
import luigi

from chipalign.core.task import MetaTask
from chipalign.database.roadmap_data.mappable_bins import RoadmapMappableBins


class FullyMappableBinsExample(MetaTask):
    cell_type = RoadmapMappableBins.cell_type
    window_size = RoadmapMappableBins.window_size

    def requires(self):
        return RoadmapMappableBins(cell_type=self.cell_type,
                                   window_size=self.window_size)


if __name__ == '__main__':
    luigi.run(main_task_cls=FullyMappableBinsExample)
