import pandas as pd

from chipalign.alignment import AlignedReadsBwa
from chipalign.alignment.consolidation import ConsolidatedReads
from chipalign.alignment.filtering import FilteredReads
from chipalign.genome.windows.genome_windows import NonOverlappingBins
from chipalign.signal.bins import BinnedSignal
from chipalign.signal.signal import Signal
from tests.helpers.decorators import slow
from tests.helpers.task_test import TaskTestCase

class TestPipeline(TaskTestCase):
    _multiprocess_can_split_ = False

    @slow
    def test_whole_pipeline(self):
        # Use drosophila datasets as they're smaller
        # Unfortunately, cannot test mappability for drosophila
        # Neither can we test blacklist.
        # But these should be covered by other tests.

        # Just random collection of drosophila chipseq things
        input_datasets = [
            ('sra', 'SRR6497754'),
            ('sra', 'SRR6497751'),
        ]

        treatment_datasets = [
            ('sra', 'SRR6497752')
        ]

        genome = 'dm6'

        limit = 100_000

        cr_input = ConsolidatedReads(
            input_alignments=[FilteredReads(
                alignment_task=AlignedReadsBwa(
                    genome_version=genome,
                    source=source,
                    accession=accession,
                    limit=limit
                ),
                ignore_non_standard_chromosomes=False,
                genome_version=genome,
                resized_length=36,
                filter_mappability=False
            ) for source, accession in input_datasets],
            use_only_standard_chromosomes=False)

        cr_treatment = ConsolidatedReads(
            input_alignments=[FilteredReads(
                alignment_task=AlignedReadsBwa(
                    genome_version=genome,
                    source=source,
                    accession=accession,
                    limit=limit
                ),
                ignore_non_standard_chromosomes=False,
                genome_version=genome,
                resized_length=36,
                filter_mappability=False
            ) for source, accession in treatment_datasets],
            use_only_standard_chromosomes=False)

        macs_signal = Signal(input_task=cr_input,
                             treatment_task=cr_treatment)

        bins_task = NonOverlappingBins(genome_version=genome,
                                       remove_blacklisted=False,
                                       window_size=10000)

        binned_signal = BinnedSignal(bins_task=bins_task,
                                     signal_task=macs_signal)

        self.build_task(binned_signal)

        df = binned_signal.output().load()

        self.assertIsInstance(df, pd.Series)
        self.assertGreater(len(df[df>0]), 0)




