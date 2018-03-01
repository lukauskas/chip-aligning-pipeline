from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import luigi

from chipalign.alignment.implementations.bowtie import AlignedReadsBowtie
from chipalign.core.task import MetaTask
from chipalign.sequence.short_reads import ShortReads


class AlignedReads(MetaTask):

    genome_version = luigi.Parameter()
    source = ShortReads.source
    accession = ShortReads.accession

    aligner = luigi.Parameter()

    def requires(self):
        if self.aligner == 'bowtie':
            class_ = AlignedReadsBowtie
        else:
            raise Exception('Aligner {} is not supported'.format(self.aligner))

        return class_(genome_version=self.genome_version,
                      source=self.source,
                      accession=self.accession)

    @property
    def self_parameters(self):
        return [self.aligner]

    def bam_output(self):
        return self.output()[0]

    @property
    def task_class_friendly_name(self):
        return 'AR'
