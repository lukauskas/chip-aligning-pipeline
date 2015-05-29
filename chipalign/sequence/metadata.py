from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from collections import Counter
import re
import luigi
from chipalign.core.file_formats.yaml_file import YamlFile
from chipalign.core.task import Task

def _parse_fastq_read_header(fastq_header):

    match = re.match('@(?P<srr_id>SRR\d+)\.(?P<read_number>\d+)\s(?P<instrument_id>[^:]+):'
                     '(?P<lane>\d+):(?P<tile>\d+):(?P<x_coord>\d+):(?P<y_coord>\d+)\s',
                     fastq_header)

    return dict(srr_identifier=match.group('srr_id'),
                read_number=int(match.group('read_number')),
                instrument_identifier=match.group('instrument_id'),
                lane=int(match.group('lane')),
                tile=int(match.group('tile')),
                x_coord=int(match.group('x_coord')),
                y_coord=int(match.group('y_coord')))


class FastqMetadata(Task):
    """
    Returns an YML of the data parsed from a FastQ file, namely the
    `srr_identifier`, `instrument_identifier`, `lane`, and `tile` information as parsed from the sequences.
    """

    # FastqSequence object as an input
    fastq_sequence = luigi.Parameter()

    def requires(self):
        return self.fastq_sequence

    @property
    def _extension(self):
        return 'yml'

    @property
    def _output_class(self):
        return YamlFile

    def run(self):

        key_columns = ['srr_identifier', 'instrument_identifier', 'lane', 'tile']

        to_key = lambda x: tuple(map(x.get, key_columns))
        counter = Counter()
        with self.fastq_sequence.output().open('r') as f:
            for line in f:
                if not line.startswith('@'):
                    continue

                counter[to_key(_parse_fastq_read_header(line))] += 1

        answer = []
        for key, count in counter.iteritems():
            d = dict(zip(key_columns, key))
            d['count'] = count
            answer.append(d)

        self.output().dump(answer)