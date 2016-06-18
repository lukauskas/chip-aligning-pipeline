from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import luigi
from chipalign.core.task import Task
from chipalign.core.util import temporary_file
from chipalign.database.encode.download import fetch_from_encode

class ShortReads(Task):
    """
    Task that takes an SRR identifier and uses `fastq_dump` utility to  download it.

    Takes three parameters:

    :param source: Source for the sequence. 'sra', or 'encode'
    :param accession: the SRR identifier of the sequence
    """
    source = luigi.Parameter()
    accession = luigi.Parameter()

    @property
    def _extension(self):
        return 'fastq.gz'

    def _from_sra(self):
        from chipalign.command_line_applications.sratoolkit import fastq_dump
        logger = self.logger()
        abspath = os.path.abspath(self.output().path)

        with self.temporary_directory():
            logger.debug('Dumping {} to fastq.gz'.format(self.accession))

            args = [self.accession, '--gzip']
            if self.spot_limit:
                args.extend(['-X', self.spot_limit])

            fastq_file = '{}.fastq.gz'.format(self.accession)

            fastq_dump(*args)

            logger.debug('Done. Moving file')
            shutil.move(fastq_file, abspath)

    def _from_encode(self):

        abspath = os.path.abspath(self.output().path)
        with temporary_file() as tf_name:
            with open(tf_name, 'w') as handle:
                fetch_from_encode(self.accession, 'fastq.dz', handle)

            shutil.move(tf_name, abspath)

    def run(self):
        self.ensure_output_directory_exists()

        if self.source == 'sra':
            self._from_sra()
        elif self.source == 'encode':
            self._from_encode()
        else:
            raise Exception('Unsupported source: {!r}'.format(self.source))

