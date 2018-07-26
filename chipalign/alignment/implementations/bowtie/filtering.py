from chipalign.alignment import AlignedReadsBowtie
from chipalign.alignment.filtering import FilteredReads
from chipalign.core.task import MetaTask


class FilteredReadsBowtie(MetaTask):

    genome_version = AlignedReadsBowtie.genome_version
    accession = AlignedReadsBowtie.accession
    source = AlignedReadsBowtie.source

    def _aligned_task(self):

        aligned_reads = AlignedReadsBowtie(genome_version=self.genome_version,
                                           accession=self.accession,
                                           source=self.source)

        filtered_reads = FilteredReads(genome_version=self.genome_version,
                                       alignment_task=aligned_reads)
        return filtered_reads

    def requires(self):
        return self._aligned_task()