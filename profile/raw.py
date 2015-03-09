from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from genome_alignment import BowtieAlignmentTask
from profile.aligned_reads_base import AlignedReadsProfileBase


class RawProfile(AlignedReadsProfileBase):

    @property
    def peaks_task(self):
        return BowtieAlignmentTask(genome_version=self.genome_version,
                                   experiment_accession=self.experiment_accession,
                                   study_accession=self.study_accession,
                                   experiment_alias=self.experiment_alias,
                                   bowtie_seed=self.bowtie_seed,
                                   pretrim_reads=self.pretrim_reads)