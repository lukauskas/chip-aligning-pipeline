from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from functools32 import lru_cache

from chipalign.alignment.aligned_reads import AlignedReads
from chipalign.alignment.consolidation import ConsolidatedReads
from chipalign.alignment.filtering import FilteredReads
from chipalign.core.task import Task, MetaTask
import luigi
import pandas as pd

from chipalign.database.encode.metadata import EncodeTFMetadata


@lru_cache(None)
def _encode_metadata(genome_version):
    task = EncodeTFMetadata(genome_version=genome_version)
    luigi.build([task])
    return pd.read_csv(task.output().path)


@lru_cache(None)
def _encode_read_accessions(genome_version, cell_type, target):
    metadata = _encode_metadata(genome_version)

    metadata = metadata.query('roadmap_cell_type == @cell_type and target == @target')
    metadata = metadata[metadata['Output type'] == 'reads']
    reads = list(metadata['File accession'].unique())

    return list(reads)


def _encode_filtered_read_tasks(genome_version, cell_type, target,
                                aligner,
                                resized_length):
    read_accessions = _encode_read_accessions(genome_version,
                                              cell_type, target)

    aligned_read_tasks = [AlignedReads(genome_version=genome_version,
                                       source='encode',
                                       accession=a,
                                       aligner=aligner) for a in read_accessions]

    filtered_read_tasks = [FilteredReads(alignment_task=a,
                                         genome_version=genome_version,
                                         resized_length=resized_length)
                           for a in aligned_read_tasks]

    return filtered_read_tasks


class EncodeConsolidatedReads(MetaTask):

    genome_version = luigi.Parameter(default='hg19')
    cell_type = luigi.Parameter()
    target = luigi.Parameter()

    aligner = luigi.Parameter(default='bowtie')

    resized_length = FilteredReads.resized_length
    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth
    subsample_random_seed = ConsolidatedReads.subsample_random_seed

    def requires(self):
        filtered_read_tasks = _encode_filtered_read_tasks(self.genome_version,
                                                          self.cell_type,
                                                          self.target,
                                                          self.aligner,
                                                          self.resized_length)

        consolidated_reads = ConsolidatedReads(
            input_alignments=filtered_read_tasks,
            max_sequencing_depth=self.max_sequencing_depth,
            subsample_random_seed=self.subsample_random_seed)

        return consolidated_reads


