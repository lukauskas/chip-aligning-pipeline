from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import shutil
import luigi
from chipalign.core.task import Task


class SRRSequence(Task):
    """
    Task that takes an SRR identifier and uses `fastq_dump` utility to  download it.

    Takes three parameters:

    :param srr_identifier: the SRR identifier of the sequence
    :param file_format: either `fastq` or `fasta`, default `fastq`
    :param spot_limit: Spot limit, equivalent to -X parameter in --fastq-dump (default: None -- no limit)
    """

    srr_identifier = luigi.Parameter()
    file_format = luigi.Parameter(default='fastq')

    # Mostly for testing purposes -- equivalent to the -X parameter in --fastq-dump
    spot_limit = luigi.IntParameter(default=None)

    @property
    def _extension(self):
        if self.file_format == 'fastq':
            return 'fastq.gz'
        elif self.file_format == 'fasta':
            return 'fasta.gz'
        else:
            raise ValueError('File format not understood: {!r}'.format(self.file_format))

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

            if self.file_format == 'fasta':
                args.append('--fasta')
                fastq_file = '{}.fasta.gz'.format(self.srr_identifier)
            elif self.file_format == 'fastq':
                fastq_file = '{}.fastq.gz'.format(self.srr_identifier)
            else:
                raise ValueError('Unsupported file format {!r}'.format(self.file_format))

            fastq_dump(*args)

            logger.debug('Done. Moving file')
            shutil.move(fastq_file, abspath)

if __name__ == '__main__':
    luigi.run(main_task_cls=SRRSequence)