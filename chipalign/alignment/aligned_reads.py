from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi
from chipalign.alignment.implementations.bowtie import AlignedReadsBowtie
from chipalign.alignment.implementations.pash import AlignedReadsPash
from chipalign.core.task import MetaTask
from chipalign.sequence.srr import SRRSequence
from chipalign.genome.genome_index import GenomeIndex


class AlignedSRR(MetaTask):

    genome_version = GenomeIndex.genome_version
    srr_identifier = SRRSequence.srr_identifier

    aligner = luigi.Parameter()

    def requires(self):
        if self.aligner == 'bowtie':
            class_ = AlignedReadsBowtie
        elif self.aligner == 'pash':
            class_ = AlignedReadsPash
        else:
            raise Exception('Aligner {} is not supported'.format(self.aligner))

        return class_(genome_version=self.genome_version, srr_identifier=self.srr_identifier)

    @property
    def self_parameters(self):
        return [self.aligner]

    def bam_output(self):
        return self.output()[0]