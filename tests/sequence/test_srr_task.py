from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from chipalign.sequence import ShortReads
from tests.helpers.task_test import TaskTestCase


class TestTestSequenceMetadataTask(TaskTestCase):

    def test_sequence_is_fetched_correctly(self):
        srr_identifier = 'SRR001392'
        spot_limit = 2

        fastq_sequence_task = ShortReads(source='sra', accession=srr_identifier,
                                         limit=spot_limit)

        expected_output = """@SRR001392.1 USI-EAS21_0019_3909:2:1:909:882 length=24
GGGGTAACGGAGGCACAGATTTAA
+SRR001392.1 USI-EAS21_0019_3909:2:1:909:882 length=24
IIIIGI<IIIIBI+C7HI<B=7:,
@SRR001392.2 USI-EAS21_0019_3909:2:1:907:899 length=24
GAATCGAAGAGAATCATCGAATGG
+SRR001392.2 USI-EAS21_0019_3909:2:1:907:899 length=24
'IIEII8IIIIIIIII8BIEI3.<\n"""

        self.build_task(fastq_sequence_task)
        self.assertTrue(fastq_sequence_task.complete())

        actual_output = fastq_sequence_task.output().open('r').read().decode('utf8')
        self.assertEqual(expected_output, actual_output)