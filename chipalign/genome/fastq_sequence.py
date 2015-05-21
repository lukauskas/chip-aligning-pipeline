from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from collections import Counter
import os
import re
import shutil

import luigi
import yaml

from chipalign.core.task import Task


class FastqSequence(Task):
    """
    Task that takes an SRR identifier and uses `fastq_dump` utility to  download it.
    """

    srr_identifier = luigi.Parameter()

    # Mostly for testing purposes -- equivalent to the -X parameter in --fastq-dump
    spot_limit = luigi.IntParameter(default=None)

    @property
    def _extension(self):
        return 'fastq.gz'

    @property
    def parameters(self):
        parameters = [self.srr_identifier]
        if self.spot_limit:
            parameters.append(self.spot_limit)

        return parameters

    def run(self):
        from chipalign.command_line_applications.sratoolkit import fastq_dump
        logger = self.logger()
        self.ensure_output_directory_exists()
        abspath = os.path.abspath(self.output().path)

        with self.temporary_directory():
            logger.debug('Dumping {} to fastq.gz'.format(self.srr_identifier))

            args = [self.srr_identifier, '--gzip']
            if self.spot_limit:
                args.extend(['-X', self.spot_limit])

            fastq_dump(*args)

            fastq_file = '{}.fastq.gz'.format(self.srr_identifier)
            logger.debug('Done. Moving file')

            shutil.move(fastq_file, abspath)

def parse_fastq_read_header(fastq_header):

    match = re.match('@(?P<srr_id>SRR\d+)\.(?P<read_number>\d+)\s(?P<instrument_id>[^:]+):(?P<lane>\d+):(?P<tile>\d+):(?P<x_coord>\d+):(?P<y_coord>\d+)\s',
                     fastq_header)

    return dict(srr_identifier=match.group('srr_id'),
                read_number=int(match.group('read_number')),
                instrument_identifier=match.group('instrument_id'),
                lane=int(match.group('lane')),
                tile=int(match.group('tile')),
                x_coord=int(match.group('x_coord')),
                y_coord=int(match.group('y_coord')))

class FastqMetadata(Task):

    # FastqSequence object as an input
    fastq_sequence = luigi.Parameter()

    def requires(self):
        return self.fastq_sequence

    @property
    def _extension(self):
        return 'yml'


    def run(self):

        key_columns = ['srr_identifier', 'instrument_identifier', 'lane', 'tile']

        to_key = lambda x: tuple(map(x.get, key_columns))
        counter = Counter()
        with self.fastq_sequence.output().open('r') as f:
            for line in f:
                if not line.startswith('@'):
                    continue

                counter[to_key(parse_fastq_read_header(line))] += 1

        answer = []
        for key, count in counter.iteritems():
            d = dict(zip(key_columns, key))
            d['count'] = count

            answer.append(d)

        with self.output().open('w') as output_:
            yaml.safe_dump(answer, output_)


if __name__ == '__main__':
    luigi.run(main_task_cls=FastqSequence)
