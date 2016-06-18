from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import luigi

from chipalign.core.file_formats.file import File
from chipalign.core.task import Task
from chipalign.core.util import ensure_directory_exists_for_file
from chipalign.sequence.short_reads import ShortReads


class AlignedReadsBase(Task):
    """
    A base class to act as a scaffold for implementing different genome aligners.

    :param genome_version: Version of genome to use
    :param srr_identifier: the SRR ID of the sequence to align

    """
    genome_version = luigi.Parameter()
    source = ShortReads.source
    accession = ShortReads.accession

    @property
    def fastq_task(self):
        return ShortReads(source=self.source, accession=self.accession)

    @property
    def index_task(self):
        raise NotImplementedError

    def requires(self):
        return [self.fastq_task, self.index_task]

    @property
    def parameters(self):
        fastq_parameters = self.fastq_task.parameters
        genome_index_parameters = self.index_task.parameters

        aligner_parameters = self.aligner_parameters

        return fastq_parameters + genome_index_parameters + aligner_parameters

    @property
    def aligner_parameters(self):
        raise NotImplementedError

    @property
    def _extension(self):
        return 'bam'

    def output(self):
        bam_output = super(AlignedReadsBase, self).output()
        stdout_output = File(bam_output.path + '.stdout')
        return bam_output, stdout_output

    def _output_abspaths(self, ensure_directory_exists=True):

        bam_output, stdout_output = self.output()
        stdout_output_abspath = os.path.abspath(stdout_output.path)
        bam_output_abspath = os.path.abspath(bam_output.path)

        if ensure_directory_exists:
            ensure_directory_exists_for_file(stdout_output_abspath)
            ensure_directory_exists_for_file(bam_output_abspath)

        return bam_output_abspath, stdout_output_abspath