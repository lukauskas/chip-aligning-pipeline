from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest
import yaml
from chipalign.sequence.metadata import _parse_fastq_read_header, FastqMetadata
from chipalign.sequence.short_reads import ShortReads
from tests.helpers.task_test import TaskTestCase


class TestSequenceMetadataExtraction(unittest.TestCase):

    def test_fastq_header_parsing(self):

        sample_header = '@SRR001392.10 USI-EAS21_0019_3909:2:1:919:931 length=24'

        parsed_data = dict(srr_identifier='SRR001392',
                           read_number=10,
                           instrument_identifier='USI-EAS21_0019_3909',
                           lane=2,
                           tile=1,
                           x_coord=919,
                           y_coord=931
                           )

        self.assertDictEqual(parsed_data, _parse_fastq_read_header(sample_header))

class TestTestSequenceMetadataTask(TaskTestCase):

    def test_fastq_sequence_metadata_task(self):
        srr_identifier = 'SRR001392'
        spot_limit = 5

        fastq_sequence_task = ShortReads(srr_identifier=srr_identifier, spot_limit=spot_limit)
        sequence_metadata_task = FastqMetadata(fastq_sequence=fastq_sequence_task)

        self.build_task(sequence_metadata_task)

        expected_data = [{'srr_identifier': srr_identifier,
                          'instrument_identifier': 'USI-EAS21_0019_3909',
                          'lane': 2,
                          'tile': 1,
                          'count': spot_limit
                          }]

        with sequence_metadata_task.output().open('r') as f:
            data = yaml.safe_load(f)

        self.assertListEqual(expected_data, data)


