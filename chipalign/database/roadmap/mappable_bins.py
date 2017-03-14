from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi

import chipalign.database.roadmap.metadata as roadmap_settings
from chipalign.core.task import MetaTask
from chipalign.genome.mappability import FullyMappableBins
from chipalign.genome.windows.genome_windows import NonOverlappingBins


class RoadmapMappableBins(MetaTask):
    cell_type = luigi.Parameter()

    genome_version = luigi.Parameter(default=roadmap_settings.GENOME_ID)
    window_size = luigi.IntParameter(default=200)
    remove_blacklisted = luigi.BoolParameter(default=True)

    read_length = luigi.IntParameter(default=roadmap_settings.READ_LENGTH)

    @property
    def max_ext_size(self):
        __, __, cell_type = self.cell_type.rpartition(':')
        return roadmap_settings.max_fraglen(cell_type)

    @property
    def bins_task(self):
        return NonOverlappingBins(genome_version=self.genome_version,
                                  window_size=self.window_size,
                                  remove_blacklisted=self.remove_blacklisted)

    def requires(self):
        return FullyMappableBins(bins_task=self.bins_task,
                                 max_ext_size=self.max_ext_size,
                                 read_length=self.read_length)
