from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from peak_calling.macs import MacsPeaks
from profile.base import ProfileBase


class AlignedReadsProfileBase(ProfileBase):
    experiment_accession = MacsPeaks.experiment_accession
    study_accession = MacsPeaks.study_accession
    experiment_alias = MacsPeaks.experiment_alias

    bowtie_seed = MacsPeaks.bowtie_seed
    pretrim_reads = MacsPeaks.pretrim_reads

    @property
    def friendly_name(self):
        return self.experiment_alias
