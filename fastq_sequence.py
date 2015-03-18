from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import luigi
import shutil
from task import Task


class FastqSequence(Task):

    srr_identifier = luigi.Parameter()

    @property
    def _extension(self):
        return 'fastq.gz'

    @property
    def parameters(self):
        return [self.srr_identifier]

    def run(self):
        from command_line_applications.sratoolkit import fastq_dump
        logger = self.logger()
        self.ensure_output_directory_exists()
        abspath = os.path.abspath(self.output().path)

        with self.temporary_directory():
            logger.debug('Dumping {} to fastq.gz'.format(self.srr_identifier))
            fastq_dump(self.srr_identifier, '--gzip')

            fastq_file = '{}.fastq.gz'.format(self.srr_identifier)
            logger.debug('Done. Moving file')

            shutil.move(fastq_file, abspath)



if __name__ == '__main__':
    luigi.run(main_task_cls=FastqSequence)
