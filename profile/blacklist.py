from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from blacklist import BlacklistedRegions
from profile.genome_wide import GenomeWideProfileBase


class BlacklistGenomeWideProfile(GenomeWideProfileBase):

    binary = True

    @property
    def features_to_map_task(self):
        return BlacklistedRegions(genome_version=self.genome_version)

    @property
    def friendly_name(self):
        return 'blacklist'