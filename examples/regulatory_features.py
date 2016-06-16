"""
Example on how to download Signal from Roadmap

Prior to running this example, just like all examples ensure that luigi scheduler is running,
by typing:

    luigid

You can then run this by

python regulatory_features.py

the output will be stored in directory configured in chipalign.yml, which in this case is output/
"""

import luigi

from chipalign.biomart.distance_to_regulatory_features import DistancesToRegulatoryFeatures
from chipalign.biomart.regulatory_features import RegulatoryFeatures
from chipalign.core.task import MetaTask
from chipalign.database.roadmap_data.mappable_bins import RoadmapMappableBins


class ExampleRegulatoryFeatures(MetaTask):
    cell_type = RegulatoryFeatures.cell_type

    def requires(self):
        return DistancesToRegulatoryFeatures(genome_version='hg19',
                                             cell_type=self.cell_type,
                                             bins_task=RoadmapMappableBins(cell_type=self.cell_type)
                                             )

if __name__ == '__main__':
    luigi.run(main_task_cls=ExampleRegulatoryFeatures)
