from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from profile.aligned_reads_mixin import AlignedReadsMixin
from profile.genome_wide import GenomeWideProfileBase


class RawProfile(GenomeWideProfileBase, AlignedReadsMixin):
    # Mixin will handle the rest
    pass