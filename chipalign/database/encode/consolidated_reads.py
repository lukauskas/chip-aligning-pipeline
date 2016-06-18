from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from chipalign.alignment.consolidation import ConsolidatedReads
from chipalign.core.task import Task, MetaTask
import luigi
import pandas as pd

from chipalign.database.encode.metadata import EncodeTFMetadata


class EncodeConsolidatedReads(Task):

    genome_version = luigi.Parameter()
    cell_type = luigi.Parameter()
    target = luigi.Parameter()

    max_sequencing_depth = ConsolidatedReads.max_sequencing_depth
    subsample_random_seed = ConsolidatedReads.subsample_random_seed

    @property
    def _extension(self):
        return 'tagAlign.gz'

    @property
    def _metadata_task(self):
        return EncodeTFMetadata(genome_version=self.genome_version)

    def requires(self):
        return self._metadata_task

    def run(self):
        metadata = pd.read_csv(self._metadata_task.output().path)

