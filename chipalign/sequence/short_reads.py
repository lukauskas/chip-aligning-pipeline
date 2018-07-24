from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import luigi
from luigi.task import flatten

from chipalign.core.task import Task
from chipalign.core.util import temporary_file
from chipalign.database.encode.download import fetch_from_encode

class ShortReads(Task):
    """
    Task that takes an SRR identifier and uses `fastq_dump` utility to  download it.

    Takes three parameters:

    :param source: Source for the sequence. 'sra', or 'encode'
    :param accession: the SRR/encode identifier of the sequence
    :param limit: Optional, only available for SRA, limit on number of reads to fetch (used for tests)
    """
    source = luigi.Parameter()
    accession = luigi.Parameter()
    limit = luigi.Parameter(default=None)

    @property
    def _extension(self):
        return 'fastq.gz'

    def complete(self):
        if not super().complete():
            return False
        else:

            # Check that short reads have non-zero size ...
            
            outputs = flatten(self.output())

            for output in outputs:
                if not os.path.isfile(output.path) or os.path.getsize(output.path) == 0:
                    return False

            return True

    def _from_sra(self):
        from chipalign.command_line_applications.sratoolkit import fastq_dump
        logger = self.logger()
        abspath = os.path.abspath(self.output().path)

        with self.temporary_directory():
            logger.debug('Dumping {} to fastq.gz'.format(self.accession))

            args = [self.accession, '--gzip']

            if self.limit is not None:
                args.extend(['-X', self.limit])

            fastq_file = '{}.fastq.gz'.format(self.accession)

            fastq_dump(*args)

            logger.debug('Done. Moving file')
            shutil.move(fastq_file, abspath)

    def _from_encode(self):
        if self.limit is not None:
            raise ValueError('Limit not available for source=encode')
        abspath = os.path.abspath(self.output().path)
        with temporary_file() as tf_name:
            with open(tf_name, 'wb') as handle:
                fetch_from_encode(self.accession, 'fastq.gz', handle)

            shutil.move(tf_name, abspath)

    def _run(self):
        self.ensure_output_directory_exists()

        if self.source == 'sra':
            self._from_sra()
        elif self.source == 'encode':
            self._from_encode()
        else:
            raise Exception('Unsupported source: {!r}'.format(self.source))

