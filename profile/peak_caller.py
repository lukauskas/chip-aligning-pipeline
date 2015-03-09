from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from peak_calling.macs import MacsPeaks
from peak_calling.rseg import RsegPeaks
from profile.aligned_reads_base import AlignedReadsProfileBase


class MacsProfile(AlignedReadsProfileBase):

    broad = MacsPeaks.broad

    @property
    def peaks_task(self):
        return MacsPeaks(genome_version=self.genome_version,
                     experiment_accession=self.experiment_accession,
                     study_accession=self.study_accession,
                     experiment_alias=self.experiment_alias,
                     bowtie_seed=self.bowtie_seed,
                     pretrim_reads=self.pretrim_reads,
                     broad=self.broad)


class RsegProfile(AlignedReadsProfileBase):

    width_of_kmers = RsegPeaks.width_of_kmers
    prefix_length = RsegPeaks.prefix_length

    number_of_iterations = RsegPeaks.number_of_iterations


    @property
    def peaks_task(self):
        return RsegPeaks(genome_version=self.genome_version,
                         experiment_accession=self.experiment_accession,
                         study_accession=self.study_accession,
                         experiment_alias=self.experiment_alias,
                         bowtie_seed=self.bowtie_seed,
                         pretrim_reads=self.pretrim_reads,
                         width_of_kmers=self.width_of_kmers,
                         prefix_length=self.prefix_length,
                         number_of_iterations=self.number_of_iterations)