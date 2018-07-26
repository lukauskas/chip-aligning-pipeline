import luigi

from chipalign.alignment.consolidation import ConsolidatedReads
from chipalign.alignment.implementations.bowtie.filtering import FilteredReadsBowtie
from chipalign.core.task import MetaTask


class ConsolidatedReadsBowtie(MetaTask):
    _parameter_names_to_hash = ('accessions_str',)

    genome_version = FilteredReadsBowtie.genome_version
    accessions_str = luigi.Parameter()
    cell_type = luigi.Parameter()

    def requires(self):
        accessions = self.accessions_str.split(';')
        filtered = []

        for source_accession in accessions:
            source, __, accession = source_accession.partition(':')

            filtered.append(FilteredReadsBowtie(source=source, accession=accession,
                                                genome_version=self.genome_version))

        return ConsolidatedReads(input_alignments=filtered)