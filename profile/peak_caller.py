from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import luigi
from peak_calling.fseq import FseqPeaks
from peak_calling.macs import MacsPeaks
from peak_calling.rseg import RsegPeaks

from profile.aligned_reads_mixin import AlignedReadsMixin
from profile.genome_wide import GenomeWideProfileBase


class MacsProfile(GenomeWideProfileBase, AlignedReadsMixin):

    broad = MacsPeaks.broad
    macs_q_value_threshold = MacsPeaks.macs_q_value_threshold

    profile_mode = luigi.Parameter(default='count')

    __MODES = {'count': dict(operation='count', null_value=0),
               'max_qvalue': dict(operation='max', null_value=0, column=9)}

    @property
    def parameters(self):
        params = super(MacsProfile, self).parameters
        if self.profile_mode == 'count':
            return params
        elif self.profile_mode in self.__MODES:
            return params + [self.profile_mode]
        else:
            raise ValueError('Unknown mode')

    def _compute_profile_kwargs(self):
        return self.__MODES[self.profile_mode]

    @property
    def features_to_map_task(self):
        return MacsPeaks(genome_version=self.genome_version,
                     experiment_accession=self.experiment_accession,
                     study_accession=self.study_accession,
                     experiment_alias=self.experiment_alias,
                     bowtie_seed=self.bowtie_seed,
                     pretrim_reads=self.pretrim_reads,
                     broad=self.broad,
                     macs_q_value_threshold=self.macs_q_value_threshold)


class RsegProfile(GenomeWideProfileBase, AlignedReadsMixin):

    width_of_kmers = RsegPeaks.width_of_kmers
    prefix_length = RsegPeaks.prefix_length

    number_of_iterations = RsegPeaks.number_of_iterations


    @property
    def features_to_map_task(self):
        return RsegPeaks(genome_version=self.genome_version,
                         experiment_accession=self.experiment_accession,
                         study_accession=self.study_accession,
                         experiment_alias=self.experiment_alias,
                         bowtie_seed=self.bowtie_seed,
                         pretrim_reads=self.pretrim_reads,
                         width_of_kmers=self.width_of_kmers,
                         prefix_length=self.prefix_length,
                         number_of_iterations=self.number_of_iterations)

class FseqProfile(GenomeWideProfileBase, AlignedReadsMixin):

    @property
    def features_to_map_task(self):
        return FseqPeaks(genome_version=self.genome_version,
                         experiment_accession=self.experiment_accession,
                         study_accession=self.study_accession,
                         experiment_alias=self.experiment_alias,
                         bowtie_seed=self.bowtie_seed,
                         pretrim_reads=self.pretrim_reads)


if __name__ == '__main__':
    import logging
    MacsProfile.logger().setLevel(logging.DEBUG)
    logging.basicConfig()
    luigi.run()